package gcppubsub

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
)

// gatedResult is a publication whose outcome arrives when the test says so.
type gatedResult struct {
	release chan struct{}
}

func newGatedResult() *gatedResult { return &gatedResult{release: make(chan struct{})} }

// Get implements psapi.Result.
func (r *gatedResult) Get(ctx context.Context) (string, error) {
	select {
	case <-r.release:
		return "id", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// doneResult is a publication whose outcome is already known.
type doneResult struct{}

// Get implements psapi.Result.
func (doneResult) Get(context.Context) (string, error) { return "id", nil }

// completionOrder records the order the callbacks ran in.
type completionOrder struct {
	mu  sync.Mutex
	got []int
}

func (o *completionOrder) done(i int) func(error) {
	return func(error) {
		o.mu.Lock()
		defer o.mu.Unlock()
		o.got = append(o.got, i)
	}
}

func (o *completionOrder) snapshot() []int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]int(nil), o.got...)
}

// TestCompletionPoolBlocksAtItsBound holds the backpressure the pool is for:
// once the worker is busy and the queue is full, the next submission waits
// rather than growing the queue, and its own context is what ends the wait.
// The completions the pool did accept still run, in submission order, when
// the worker is freed.
func TestCompletionPoolBlocksAtItsBound(t *testing.T) {
	const queue = 2
	p, err := newCompletionPool(1, queue)
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()

	var order completionOrder
	busy := newGatedResult()
	ctx := context.Background()
	if err := p.submit(ctx, completion{res: busy, done: order.done(0)}); err != nil {
		t.Fatalf("submit 0: %v", err)
	}
	// The worker takes the first completion and parks on it, so the queue
	// itself is empty again; wait for that rather than assume it.
	waitCompletion(t, func() bool { return p.queued() == 0 })
	for i := 1; i <= queue; i++ {
		if err := p.submit(ctx, completion{res: doneResult{}, done: order.done(i)}); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}

	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	err = p.submit(short, completion{res: doneResult{}, done: order.done(99)})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a submission to a full pool returned %v, want context.DeadlineExceeded", err)
	}

	close(busy.release)
	waitCompletion(t, func() bool { return len(order.snapshot()) == queue+1 })
	got := order.snapshot()
	for i, v := range got {
		if v != i {
			t.Fatalf("callbacks ran in the order %v, want submission order", got)
		}
	}
}

// TestCompletionPoolCloseWakesAParkedSubmission holds the other way out of a
// full queue: closing the pool refuses a submission that was waiting with
// ErrClosed and does not call its callback, while every completion the pool
// had accepted is drained and called exactly once.
func TestCompletionPoolCloseWakesAParkedSubmission(t *testing.T) {
	p, err := newCompletionPool(1, 1)
	if err != nil {
		t.Fatal(err)
	}

	var order completionOrder
	busy := newGatedResult()
	ctx := context.Background()
	if err := p.submit(ctx, completion{res: busy, done: order.done(0)}); err != nil {
		t.Fatalf("submit 0: %v", err)
	}
	waitCompletion(t, func() bool { return p.queued() == 0 })
	if err := p.submit(ctx, completion{res: doneResult{}, done: order.done(1)}); err != nil {
		t.Fatalf("submit 1: %v", err)
	}

	parked := make(chan error, 1)
	go func() {
		parked <- p.submit(ctx, completion{res: doneResult{}, done: order.done(2)})
	}()
	// Give the third submission time to park on the full queue. If it has
	// not parked yet it is refused at the closed check instead, which is
	// the same answer.
	time.Sleep(20 * time.Millisecond)

	closed := make(chan struct{})
	go func() {
		p.close()
		close(closed)
	}()
	select {
	case err := <-parked:
		if !errors.Is(err, mq.ErrClosed) {
			t.Fatalf("a submission parked across close returned %v, want ErrClosed", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("close left a submission parked on the full queue")
	}
	// close drains rather than discards, so it waits for the busy
	// publication's outcome; the parked submission was refused before that.
	close(busy.release)
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("close did not return")
	}
	got := order.snapshot()
	if len(got) != 2 || got[0] != 0 || got[1] != 1 {
		t.Fatalf("callbacks %v ran, want exactly the two accepted ones, 0 then 1", got)
	}
	if err := p.submit(ctx, completion{res: doneResult{}, done: order.done(3)}); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("a submission after close returned %v, want ErrClosed", err)
	}
}

// countingPublisher is a publisher that records how many publications
// reached it.
type countingPublisher struct {
	mu    sync.Mutex
	calls int
}

// Publish implements psapi.Publisher.
func (p *countingPublisher) Publish(context.Context, *psapi.Outgoing) psapi.Result {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	return doneResult{}
}

// ResumePublish implements psapi.Publisher.
func (*countingPublisher) ResumePublish(string) {}

// Flush implements psapi.Publisher.
func (*countingPublisher) Flush() {}

// Stop implements psapi.Publisher.
func (*countingPublisher) Stop() {}

func (p *countingPublisher) published() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

// TestPublishAsyncRefusedByAFullPoolSendsNothing holds the order of the
// asynchronous path: a publication the completion pool has no room for is
// refused before the library sees it, so the refusal the caller gets is a
// definite one and not a message the library sends anyway with no callback.
func TestPublishAsyncRefusedByAFullPoolSendsNothing(t *testing.T) {
	p, err := newCompletionPool(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	busy := newGatedResult()
	defer close(busy.release)
	ctx := context.Background()
	if err := p.submit(ctx, completion{res: busy}); err != nil {
		t.Fatalf("submit busy: %v", err)
	}
	waitCompletion(t, func() bool { return p.queued() == 0 })
	if err := p.submit(ctx, completion{res: doneResult{}}); err != nil {
		t.Fatalf("submit filler: %v", err)
	}

	pub := &countingPublisher{}
	s := &sink{
		b:   &Backend{pool: p},
		enc: mustEncoder(t, Config{ProjectID: "p"}, mq.MetadataPortableEnvelope),
		pub: pub,
	}
	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	err = s.PublishAsync(short, &mq.Message{Body: []byte("x")}, &mq.PublishOptions{}, func(error) {
		t.Error("done ran for a refused publication")
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("PublishAsync to a full pool returned %v, want context.DeadlineExceeded", err)
	}
	if n := pub.published(); n != 0 {
		t.Fatalf("a refused publication reached the library %d times, want 0", n)
	}
}

// queued is how many accepted completions no worker has taken yet.
func (p *completionPool) queued() int {
	p.qmu.Lock()
	defer p.qmu.Unlock()
	return p.count
}

// waitCompletion polls for a condition the pool's workers establish.
func waitCompletion(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("the condition did not hold within 10s")
}

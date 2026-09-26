package gcppubsub

import (
	"context"
	"sync"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
	"github.com/iqhive/iqmq/internal/errcode"
)

// completionPool drains [psapi.Result] for publications nobody is waiting on.
//
// # Why it exists
//
// The client library's Publish is asynchronous: it bundles the message and
// returns a pending result whose Get blocks until the service answers. The
// obvious way to serve the SPI's asynchronous and owned publish paths is
// therefore
//
//	go func() { _, err := res.Get(ctx); done(err) }()
//
// which is a goroutine per message — rule 3's prohibition, verbatim. A
// publisher under load would hold one goroutine per outstanding publication,
// on top of the one the library holds per bundler and per gRPC stream.
//
// So the completions go through a FIXED pool: [Config.CompletionWorkers]
// goroutines, a queue bounded by [Config.CompletionQueue], and a submission
// that blocks when the queue is full. The specification requires every queue to be
// bounded, and the block is the backpressure: an application publishing
// asynchronously faster than the service answers waits, rather than growing a
// queue nobody bounded.
//
// # What a synchronous publication does instead
//
// Nothing. A synchronous Publish calls Get on the CALLER's goroutine, because
// the caller is already blocked waiting for the answer: routing it through the
// pool would add a hand-off, a channel and a wake-up to a path that has a
// goroutine of its own by definition. The pool exists for the completions
// nobody is waiting on, which is exactly the set that would otherwise need a
// goroutine created for them.
type completionPool struct {
	queue chan completion
	// workers is the worker goroutines' WaitGroup, and subs is the
	// in-flight SUBMISSIONS'. The two are separate because close has to
	// wait for the submissions BEFORE it closes the queue — a send on a
	// closed channel is a panic — and for the workers afterwards.
	workers sync.WaitGroup
	subs    sync.WaitGroup

	// mu guards closed and gates subs.Add, so that no submission can begin
	// after close has decided to wait for them.
	mu     sync.Mutex
	closed bool

	// stop wakes a submission parked on a full queue.
	stop chan struct{}
	// ctx is what every Get is called with, so that a Get in flight when the
	// pool closes is cancelled rather than waited for indefinitely.
	ctx    context.Context
	cancel context.CancelFunc
}

// completion is one pending publication and what to do with its outcome.
type completion struct {
	res psapi.Result
	// pub is the publisher the result came from, so that an ordered
	// publication that failed can have its key resumed. It is nil for an
	// unordered publication.
	pub psapi.Publisher
	// key is the ordering key to resume on failure, empty for none.
	key string
	// done is the callback. The pool calls it exactly once for every
	// completion it accepted, and never for one it refused.
	done func(error)
}

// newCompletionPool starts the workers.
func newCompletionPool(workers, queue int) (*completionPool, error) {
	if workers <= 0 || queue <= 0 {
		return nil, errPoolSize
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &completionPool{
		queue:  make(chan completion, queue),
		stop:   make(chan struct{}),
		ctx:    ctx,
		cancel: cancel,
	}
	p.workers.Add(workers)
	for range workers {
		go p.work()
	}
	return p, nil
}

// submit hands one completion to the pool, blocking while the queue is full.
//
// It returns ErrClosed once the pool is closed and ctx.Err() when the caller's
// context is done, and in NEITHER case does it call done: the caller still owns
// the callback, which is what keeps the specification's "exactly once" true through a
// refused submission.
func (p *completionPool) submit(ctx context.Context, c completion) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return mq.ErrClosed
	}
	// The registration happens under the lock that close takes before it
	// waits, so a submission either starts before close decides to wait or
	// is refused. Nothing can be sending when the queue is closed.
	p.subs.Add(1)
	p.mu.Unlock()
	defer p.subs.Done()
	select {
	case p.queue <- c:
		return nil
	case <-p.stop:
		return mq.ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// work is one completion worker. It exits when the queue is closed, having
// drained everything already in it.
func (p *completionPool) work() {
	defer p.workers.Done()
	for c := range p.queue {
		p.finish(c)
	}
}

// finish waits for one publication's outcome, resumes a paused ordering key,
// and calls the callback exactly once.
func (p *completionPool) finish(c completion) {
	_, err := c.res.Get(p.ctx)
	if err != nil && c.key != "" && c.pub != nil {
		resumeKey(c.pub, c.key)
	}
	if c.done != nil {
		c.done(err)
	}
}

// resumeKey re-enables publication for an ordering key whose publication
// failed.
//
// It is UNCONDITIONAL on a failure and not conditional on the error being the
// library's paused-key one, and that is the whole point. The library pauses a
// key on ANY failure of an ordered publication, and it does not report the
// pause to the publication that caused it — only to the NEXT one, and then to
// every one after that for the life of the publisher. A driver that resumed
// only on the paused-key error would therefore never resume at all: the first
// failure is a plain service error, and by the time a paused-key error
// arrives, every publication of that key since the first failure has already
// been refused.
//
// Not resuming is the message loss in slow motion — a
// publisher that has silently stopped carrying one key while every other key
// still works — which is why this is a named function with a comment rather
// than a line inside a branch.
func resumeKey(pub psapi.Publisher, key string) {
	if pub == nil || key == "" {
		return
	}
	pub.ResumePublish(key)
}

// close stops accepting completions, drains what was already submitted and
// waits for the workers.
//
// The queue is DRAINED rather than discarded: a completion already submitted
// is one whose callback the core is waiting for, and dropping it would leave an
// asynchronous publication with no outcome at all.
func (p *completionPool) close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.stop)
	p.mu.Unlock()
	// No submission can begin now, so waiting for the ones in flight is
	// bounded by the queue draining, and the queue is only closed once
	// nothing can send to it.
	p.subs.Wait()
	close(p.queue)
	p.workers.Wait()
	p.cancel()
}

var errPoolSize = errcode.New(errcode.InvalidConfig,
	"iqmq/gcppubsub: the completion pool needs a positive worker count and a positive queue bound")

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
// It waits on its own goroutine. A synchronous Publish calls Get on the
// CALLER's goroutine, because the caller is already blocked waiting for the
// answer: routing it through the pool would add a hand-off, a queue and a
// wake-up to a path that has a goroutine of its own by definition. The pool
// exists for the completions nobody is waiting on, which is exactly the set
// that would otherwise need a goroutine created for them.
//
// One case does take a place: an ORDERED synchronous publication (Publish,
// PublishBatch, PublishOwned with an ordering key) under a context that can
// end reserves one for the length of its call ([sink.holdPlace]), and
// commits a completion in it only when its wait ends before the library
// answers, to count the answer against its key ([keyLedger]). It gives the
// place back otherwise. The reservation is bounded by its context, as
// PublishAsync's is, and with a small queue such publications compete for
// places with PublishAsync and with each other.
type completionPool struct {
	// The queue is a ring of accepted completions under qmu, oldest first,
	// and held is the places reserved for completions not committed yet.
	// A worker waits on arrived for one to arrive or for the queue to be
	// finished; a submission parked on a full ring waits on room, which
	// every worker that takes a completion signals.
	qmu      sync.Mutex
	arrived  sync.Cond
	ring     []completion
	head     int
	count    int
	held     int
	finished bool
	room     chan struct{}

	// workers is the worker goroutines' WaitGroup, and subs is the
	// in-flight SUBMISSIONS'. The two are separate because close has to
	// wait for the submissions BEFORE it finishes the queue — a completion
	// accepted after the workers were told to stop would never be called —
	// and for the workers afterwards.
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
	// done is the callback, given the service's message identifier on
	// success. The pool calls it exactly once for every completion it
	// accepted, and never for one it refused. It is also where the sink
	// counts the answer against the publication's ordering key
	// ([keyLedger]).
	done func(id string, err error)
}

// newCompletionPool starts the workers.
func newCompletionPool(workers, queue int) (*completionPool, error) {
	if workers <= 0 || queue <= 0 {
		return nil, errPoolSize
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &completionPool{
		ring: make([]completion, queue),
		// One token: a parked submission that takes it and still finds
		// room after its own push passes it on, so every parked
		// submission is woken in turn without a token per slot.
		room:   make(chan struct{}, 1),
		stop:   make(chan struct{}),
		ctx:    ctx,
		cancel: cancel,
	}
	p.arrived.L = &p.qmu
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
	if err := p.reserve(ctx); err != nil {
		return err
	}
	p.commit(c)
	return nil
}

// reserve holds one place in the queue, blocking while the queue is full, for
// a completion whose result does not exist yet. Every successful reserve must
// be followed by exactly one commit or unreserve.
//
// It is what lets a publication be refused BEFORE it reaches the library: a
// publication handed to the library first and refused a place afterwards is
// one the library still sends while its caller is told it was refused, whose
// callback never runs and whose ordering key is never resumed. It fails the
// way submit does.
func (p *completionPool) reserve(ctx context.Context) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return mq.ErrClosed
	}
	// The registration happens under the lock that close takes before it
	// waits, so a reservation either starts before close decides to wait or
	// is refused. Nothing can be holding one when the queue is finished:
	// commit is what ends the registration.
	p.subs.Add(1)
	p.mu.Unlock()
	parked := false
	for {
		if p.hold(parked) {
			return nil
		}
		parked = true
		select {
		case <-p.room:
		case <-p.stop:
			p.subs.Done()
			return mq.ErrClosed
		case <-ctx.Done():
			p.subs.Done()
			return ctx.Err()
		}
	}
}

// hold takes one place in the ring for a later commit, or reports that the
// ring is full. A reservation that had parked passes the room token on when
// there is still room after its own, because the one token may have been sent
// for more than one free slot.
func (p *completionPool) hold(parked bool) bool {
	p.qmu.Lock()
	if p.count+p.held == len(p.ring) {
		p.qmu.Unlock()
		return false
	}
	p.held++
	more := p.count+p.held < len(p.ring)
	p.qmu.Unlock()
	if parked && more {
		p.signalRoom()
	}
	return true
}

// unreserve gives back a place reserve held and nothing was committed to, and
// ends the reservation as commit would. It is for a synchronous publication
// that held a place in case its wait ended, and was answered in time
// ([sink.holdPlace]).
func (p *completionPool) unreserve() {
	p.qmu.Lock()
	p.held--
	p.qmu.Unlock()
	p.signalRoom()
	p.subs.Done()
}

// commit appends c to the ring in the place reserve held for it.
func (p *completionPool) commit(c completion) {
	p.qmu.Lock()
	p.held--
	tail := p.head + p.count
	if tail >= len(p.ring) {
		tail -= len(p.ring)
	}
	p.ring[tail] = c
	p.count++
	p.arrived.Signal()
	p.qmu.Unlock()
	p.subs.Done()
}

// next takes the oldest accepted completion, waiting for one. It reports
// false once the queue is finished and empty.
func (p *completionPool) next() (completion, bool) {
	p.qmu.Lock()
	for p.count == 0 && !p.finished {
		p.arrived.Wait()
	}
	if p.count == 0 {
		p.qmu.Unlock()
		return completion{}, false
	}
	c := p.ring[p.head]
	p.ring[p.head] = completion{}
	p.head++
	if p.head == len(p.ring) {
		p.head = 0
	}
	p.count--
	p.qmu.Unlock()
	p.signalRoom()
	return c, true
}

// signalRoom wakes one submission parked on a full ring, if any is.
func (p *completionPool) signalRoom() {
	select {
	case p.room <- struct{}{}:
	default:
	}
}

// work is one completion worker. It exits when the queue is finished,
// having drained everything already in it.
func (p *completionPool) work() {
	defer p.workers.Done()
	for {
		c, ok := p.next()
		if !ok {
			return
		}
		p.finish(c)
	}
}

// finish waits for one publication's outcome and calls the callback exactly
// once.
func (p *completionPool) finish(c completion) {
	id, err := c.res.Get(p.ctx)
	if c.done != nil {
		c.done(id, err)
	}
}

// keyLedger is a sink's ordering-key resume. It counts, per ordering key, the
// publications the sink has handed the client library and not yet had the
// library's answer to, and resumes a key whose publication failed once that
// count is back to zero.
//
// # Why the key is resumed at all
//
// The library pauses a key on ANY failure of an ordered publication, and it
// does not report the pause to the publication that caused it: only to the
// later ones, for the life of the publisher. A driver that never resumed would
// have a publisher that has silently stopped carrying one key while every
// other key still works, which is the message loss in slow motion. And the
// resume follows ANY failure, not only the paused-key one: the first failure
// is a plain service error, and by the time a paused-key error arrives, every
// publication of the key since the first failure has been refused.
//
// # Why it waits
//
// The library refuses the publications of a paused key that it still holds
// only as each comes up to be sent. The driver used to resume the key as soon
// as it read the failure, which can be before the library takes the next
// publication of the key up: that one was then sent while the failed one was
// not, and the caller's retry of the failure landed behind it. Resumed once
// the library has answered every publication of the key handed to it, the key
// keeps its order: each publication behind the failure was refused with the
// paused-key error, which is retryable, so the caller retries them all in
// order, and nothing of the key is in the library to overtake the retry.
//
// The cost is to a publisher that keeps a failed key busy: while a
// publication of the key is always outstanding the count never reaches zero,
// and every publication of the key gets the paused-key error until the
// publisher lets the key go quiet. Nothing is lost or reordered meanwhile, and
// a caller that stops to retry the failure in order is that pause. A producer
// that keeps one key saturated, such as a bulk replay, gets every publication
// of it refused until it pauses, so it should stop that key at its first
// failure.
//
// A synchronous publication whose wait ended is counted until the library
// answers it, by the completion pool, in a place the publication took before
// it was handed over ([sink.holdPlace]).
type keyLedger struct {
	mu   sync.Mutex
	keys map[string]keyCount
}

// keyCount is one ordering key's publications in the library.
type keyCount struct {
	// open is how many publications of the key were handed over and not
	// answered yet.
	open int
	// failed is set once one of them has failed, and resumes the key when
	// open is back to zero.
	failed bool
}

// admit counts one publication of key, before it is handed to the library.
// The empty key is no key and is not counted.
func (l *keyLedger) admit(key string) {
	if key == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.keys == nil {
		l.keys = make(map[string]keyCount)
	}
	c := l.keys[key]
	c.open++
	l.keys[key] = c
}

// answer counts the library's answer to one publication of key, failed or
// not, and resumes the key on pub when it was the last one in the library and
// one of them had failed.
//
// The resume happens under the ledger's lock. A publication of the key
// admitted while it ran would otherwise be counted after the decision to
// resume, and could then fail and be overtaken by one behind it that the late
// resume let through.
func (l *keyLedger) answer(pub psapi.Publisher, key string, failed bool) {
	if key == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	c := l.keys[key]
	c.open--
	c.failed = c.failed || failed
	if c.open > 0 {
		l.keys[key] = c
		return
	}
	delete(l.keys, key)
	if c.failed {
		pub.ResumePublish(key)
	}
}

// resumeKey re-enables publication for an ordering key whose publication to
// the dead-letter topic failed (see [source.republish]); why it is resumed at
// all is [keyLedger]'s first section. It is resumed as soon as the failure is
// read, without the ledger's wait, so a copy of the same key that a concurrent
// rejection handed the library can still be sent ahead of a retry of the
// failed one.
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
	// bounded by the queue draining, and the queue is only finished once
	// nothing can add to it.
	p.subs.Wait()
	p.qmu.Lock()
	p.finished = true
	p.arrived.Broadcast()
	p.qmu.Unlock()
	p.workers.Wait()
	p.cancel()
}

var errPoolSize = errcode.New(errcode.InvalidConfig,
	"iqmq/gcppubsub: the completion pool needs a positive worker count and a positive queue bound")

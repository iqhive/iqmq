package driverkit

import (
	"context"
	"sync"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/errcode"
)

// PushConfig configures a [PushAdapter].
type PushConfig struct {
	// Capacity is the most deliveries the ring holds. It is required and
	// must be positive: an unbounded push adapter is the defect this type
	// exists to prevent. A driver sizes it from its prefetch.
	Capacity int
	// MaxBytes bounds the accounted payload bytes the ring holds. Zero means
	// no byte bound, which is the right answer for a broker whose own flow
	// control is already in messages.
	MaxBytes int64
	// Size measures one delivery for the byte bound. Nil means
	// [DeliverySize], which counts the key, the body and the headers.
	Size func(*mq.DriverDelivery) int64
}

// PushAdapter turns a callback-delivering broker into the SPI's pull Fetch
//
// A go-nsq handler, a Pub/Sub streaming pull and a Pulsar listener all deliver
// by calling the driver. The SPI asks the driver to be called instead. The ring
// in between is what makes the two meet, and its bound is what makes the
// meeting safe: [PushAdapter.Push] blocks while the ring is full, and a broker
// whose callback is blocked stops delivering. That is the backpressure. A push
// adapter that buffered without bound would turn a slow consumer into unbounded
// memory growth, which the specification forbids ("all queues bounded") and which is the
// single most common defect in hand-written push-to-pull adapters.
//
// One delivery always fits. A delivery larger than MaxBytes is admitted into an
// empty ring rather than blocking for room that can never appear, because the
// alternative is a deadlock whose cause is a byte bound the operator set.
//
// Close wakes both sides with [github.com/iqhive/iqmq.ErrClosed], which is how
// the specification's "Close unblocks blocked operations" reaches a driver, and
// [PushAdapter.Drain] hands back what the ring still held so that a closing
// source can requeue or abandon it.
//
// Every method is safe for concurrent use. There is one Fetch at a time per
// source, but nothing here depends on it.
type PushAdapter struct {
	mu       sync.Mutex
	ring     []mq.DriverDelivery
	head     int
	count    int
	bytes    int64
	maxBytes int64
	size     func(*mq.DriverDelivery) int64
	blocked  int
	closed   bool
	notEmpty broadcastSignal
	notFull  broadcastSignal
}

// NewPushAdapter returns a push adapter with the configured bounds.
func NewPushAdapter(cfg PushConfig) (*PushAdapter, error) {
	if cfg.Capacity <= 0 {
		return nil, errPushCapacity
	}
	if cfg.MaxBytes < 0 {
		return nil, errPushBytes
	}
	size := cfg.Size
	if size == nil {
		size = DeliverySize
	}
	return &PushAdapter{
		ring:     make([]mq.DriverDelivery, cfg.Capacity),
		maxBytes: cfg.MaxBytes,
		size:     size,
	}, nil
}

// Push admits one delivery, blocking while the ring is full.
//
// It is what a native callback calls, and the block is the point: the broker's
// delivery goroutine waits here until a consumer has taken something out, so
// the broker's own flow control sees a slow consumer. It returns ErrClosed once
// the adapter is closed and ctx.Err() when the callback's context is done.
//
// The CONTEXT IS CHECKED BEFORE EVERY INSERT and not only when the ring is
// full, which is what makes the sentence above true of a push that has room.
// It used to be consulted only in the `select` that waits for room, so two
// admissions slipped past a cancelled context: a push that found room went
// straight in, and a push that was already parked could take the `notFull`
// branch of a `select` whose other branch was ready too -- and `Drain`
// broadcasts `notFull`, so the parked push was woken by the very drain it then
// inserted behind. Drivers cancel this context to mean "the connection under
// these deliveries is gone, admit no more": `drivers/rabbitmq`'s
// `source.claimLost` cancels its pump and then drains, and documents the order
// as the thing that closes the window. It did not close it, and the delivery
// that landed in the drained ring was handed to a caller who could not settle
// it. Checking here does not make the driver's own guard
// unnecessary -- a push can still be descheduled between this check and the
// insert -- it removes the two cases the guard would otherwise be the only
// answer to.
func (p *PushAdapter) Push(ctx context.Context, d mq.DriverDelivery) error {
	size := p.size(&d)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return mq.ErrClosed
		}
		if p.roomLocked(size) {
			p.ring[(p.head+p.count)%len(p.ring)] = d
			p.count++
			p.bytes += size
			broadcast(&p.notEmpty)
			p.mu.Unlock()
			return nil
		}
		p.blocked++
		ch := listen(&p.notFull)
		p.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			p.mu.Lock()
			p.blocked--
			p.mu.Unlock()
			return ctx.Err()
		}
		p.mu.Lock()
		p.blocked--
		p.mu.Unlock()
	}
}

// TryPush admits one delivery without blocking, reporting whether there was
// room. It is for a client whose callback must not block at all and that has
// its own way of declining a message.
func (p *PushAdapter) TryPush(d mq.DriverDelivery) (bool, error) {
	size := p.size(&d)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false, mq.ErrClosed
	}
	if !p.roomLocked(size) {
		return false, nil
	}
	p.ring[(p.head+p.count)%len(p.ring)] = d
	p.count++
	p.bytes += size
	broadcast(&p.notEmpty)
	return true, nil
}

// roomLocked reports whether a delivery of size bytes fits. An empty ring always
// has room, so an oversized delivery cannot deadlock the adapter.
func (p *PushAdapter) roomLocked(size int64) bool {
	if p.count >= len(p.ring) {
		return false
	}
	if p.maxBytes > 0 && p.count > 0 && p.bytes+size > p.maxBytes {
		return false
	}
	return true
}

// Fetch fills dst(n] with the deliveries the ring holds.
//
// It is one blocking wait and then a non-blocking drain: it waits until at
// least one delivery is available, ctx is done or the adapter is closed, and
// then takes everything that is there rather than waiting for dst to fill. A
// short return is normal, which is what the SPI says of every Fetch.
//
// It allocates nothing: the deliveries are copied into the caller's slice.
func (p *PushAdapter) Fetch(ctx context.Context, dst []mq.DriverDelivery) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	for {
		p.mu.Lock()
		if p.count > 0 {
			n := p.drainLocked(dst)
			broadcast(&p.notFull)
			p.mu.Unlock()
			return n, nil
		}
		if p.closed {
			p.mu.Unlock()
			return 0, mq.ErrClosed
		}
		ch := listen(&p.notEmpty)
		p.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

// drainLocked moves up to len(dst) deliveries out of the ring.
func (p *PushAdapter) drainLocked(dst []mq.DriverDelivery) int {
	n := min(p.count, len(dst))
	for i := range n {
		slot := (p.head + i) % len(p.ring)
		dst[i] = p.ring[slot]
		p.bytes -= p.size(&p.ring[slot])
		p.ring[slot] = mq.DriverDelivery{}
	}
	p.head = (p.head + n) % len(p.ring)
	p.count -= n
	if p.count == 0 {
		// Accounting drift would otherwise accumulate across a Size
		// function that is not exactly reproducible.
		p.bytes = 0
	}
	return n
}

// Close closes the adapter and wakes everything parked in Push or Fetch with
// ErrClosed. It is idempotent, and it does not discard what the ring
// holds: [PushAdapter.Drain] is what a closing source requeues with.
func (p *PushAdapter) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	broadcast(&p.notEmpty)
	broadcast(&p.notFull)
	return nil
}

// Closed reports whether the adapter is closed.
func (p *PushAdapter) Closed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

// Drain hands every delivery the ring still holds to fn, in the order they were
// pushed, and empties the ring. It is what a closing source requeues or abandons
// the unfetched deliveries with (the specification's "Close requeues or abandons everything it
// has handed out").
//
// fn is called outside the adapter's lock, so it may do whatever a requeue
// needs — including calling back into the driver.
func (p *PushAdapter) Drain(fn func(*mq.DriverDelivery)) int {
	p.mu.Lock()
	n := p.count
	var held []mq.DriverDelivery
	if n > 0 {
		held = make([]mq.DriverDelivery, n)
		p.drainLocked(held)
	}
	broadcast(&p.notFull)
	p.mu.Unlock()
	if fn != nil {
		for i := range held {
			fn(&held[i])
		}
	}
	return n
}

// Len is how many deliveries the ring holds.
func (p *PushAdapter) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.count
}

// Bytes is the accounted payload size of what the ring holds.
func (p *PushAdapter) Bytes() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.bytes
}

// Blocked is how many pushes are parked waiting for room. It is the number a
// test asserts on to show that the broker really is being held off: a push
// adapter whose Blocked is always zero under a full ring is not applying
// backpressure, it is buffering.
func (p *PushAdapter) Blocked() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.blocked
}

// DeliverySize is the default byte accounting: the key, the body and the header
// keys and values. It is what a broker's own flow control counts, near enough,
// and it never reads anything the driver does not own.
func DeliverySize(d *mq.DriverDelivery) int64 {
	if d == nil || d.Message == nil {
		return 0
	}
	n := int64(len(d.Message.Key)) + int64(len(d.Message.Body))
	for k, v := range d.Message.Headers.All() {
		n += int64(len(k)) + int64(len(v))
	}
	return n
}

// broadcastSignal is the channel listen hands out and broadcast closes. It is
// nil until someone listens.
type broadcastSignal struct {
	ch chan struct{}
}

// listen returns a channel that is closed the next time broadcast is called on
// the same signal. Both are called with the adapter's lock held.
//
// It is the broadcast a sync.Cond would give, in a form a select can wait on
// beside a context: a Push that cannot be cancelled by its callback's context
// is a goroutine a closing client cannot get rid of.
func listen(s *broadcastSignal) chan struct{} {
	if s.ch == nil {
		s.ch = make(chan struct{})
	}
	return s.ch
}

// broadcast wakes every waiter on the signal.
func broadcast(s *broadcastSignal) {
	if s.ch != nil {
		close(s.ch)
		s.ch = nil
	}
}

// The push adapter's construction failures.
var (
	errPushCapacity = errcode.New(errcode.InvalidConfig, "iqmq/driverkit: a PushAdapter needs a positive Capacity")
	errPushBytes    = errcode.New(errcode.InvalidConfig, "iqmq/driverkit: a PushAdapter's MaxBytes cannot be negative")
)

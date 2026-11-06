package pulsar

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "github.com/iqhive/iqmq"
)

// room is the places in a producer's queue under [Config.BlockIfQueueFull],
// which a publication takes before it is handed to the producer and gives back
// once the producer has answered it, and the publications waiting for one,
// first come first served.
//
// The wait is this driver's own because pulsar-client-go v0.21's is not safe
// to give up on. Its publish semaphore counts a waiter in before it waits and
// out once its context ends, and its Release offers the place it frees on an
// unbuffered channel to a waiter it counted. When every waiter it counted has
// left, nothing takes the offer and the Release waits for ever: on a
// synchronous Send a Close ended, which held the Close for ever, or on the
// connection's reader, which then read no receipt again for any producer or
// consumer on that connection. So the driver always sets
// DisableBlockIfQueueFull, and a publication waits here, where giving up hands
// back nothing it did not take.
//
// The client can still refuse a publication the room let through
// ([refusalOf]). That publication keeps its place and joins the line,
// and while anybody is in the line nobody else is let in. Each place that
// comes back wakes one publication in the line, the first not woken yet, which
// leaves the line to be handed to the client again; refused again, it goes
// back to the front. The first in the line also tries again after a pause,
// for room the client frees without a place coming back. Only once the line
// is empty do the places that came back meanwhile go to the waiters. A
// refused publication used to give its place back and wait at the end, and
// the publications that came after it went first, so under a full memory
// buffer the slowest one waited seconds.
//
// A nil room is no wait at all, which is a producer without BlockIfQueueFull.
type room struct {
	mu sync.Mutex
	// per is the places for each partition, and size the places in all: per
	// times the most partitions the producer has had. partitions is how many
	// it has now, or nil for a producer that does not say, which has one.
	per        int
	size       int
	partitions func() int
	free       int
	// waiters is closed, first to last, as places are handed to them. A
	// place is free only when nobody is waiting for it and nobody is in the
	// line.
	waiters []chan struct{}
	// line is the publications the client refused, first to last, less the
	// ones out for a try. gives counts the places given back, so that one
	// refused again can tell whether a place came back during its try.
	line  []*retrier
	gives uint64
	// abandonedAt is when a synchronous Send last returned on its context
	// ([room.abandoned]).
	abandonedAt time.Time
}

// retrier is one publication in a room's line. wake carries the one wake-up a
// place that came back gives it, and woken says that it has one waiting.
// inLine says whether it is in the line or out for a try, and gives is the
// room's count when its last try began. All but wake are guarded by the
// room's mutex.
type retrier struct {
	wake   chan struct{}
	woken  bool
	inLine bool
	gives  uint64
}

// newRoom is a room of per places for each partition prod has, which grows as
// partitions are added, or no room at all when per is zero.
//
// v0.21 bounds each partition's queue at MaxPendingMessages and chooses the
// partition inside the producer, after the wait, so the driver cannot keep a
// room for each partition. Its room holds the same number in all, and the
// client refuses what one partition cannot take, which the line waits out.
// pulsar-client-go's partitioned producer says how many partitions it has now
// (NumPartitions), and a producer that does not say has one.
func newRoom(per int, prod pulsar.Producer) *room {
	if per == 0 {
		return nil
	}
	r := &room{per: per, size: per, free: per}
	if p, ok := prod.(interface{ NumPartitions() uint32 }); ok {
		r.partitions = func() int { return int(p.NumPartitions()) }
		r.grow()
	}
	return r
}

// grow adds the places of partitions added since it last looked. The caller
// holds r.mu.
func (r *room) grow() {
	if r.partitions == nil {
		return
	}
	n := r.per * max(r.partitions(), 1)
	if n <= r.size {
		return
	}
	added := n - r.size
	r.free += added
	r.size = n
	if len(r.line) == 0 {
		r.handOut()
		return
	}
	// The partitions added may have room for the line, which nobody would
	// otherwise wake for them.
	for range added {
		if !r.wakeNext() {
			return
		}
	}
}

// handOut hands free places to the waiters, first to last, unless somebody is
// in the line. The caller holds r.mu.
func (r *room) handOut() {
	for r.free > 0 && len(r.waiters) > 0 && len(r.line) == 0 {
		r.free--
		close(r.waiters[0])
		r.waiters = r.waiters[1:]
	}
}

// take waits for a place until ctx ends or stop is closed, which it reports as
// ctx's error or ErrClosed.
func (r *room) take(ctx context.Context, stop <-chan struct{}) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	r.grow()
	if r.free > 0 && len(r.waiters) == 0 && len(r.line) == 0 {
		r.free--
		r.mu.Unlock()
		return nil
	}
	ready := make(chan struct{})
	r.waiters = append(r.waiters, ready)
	r.mu.Unlock()
	var err error
	select {
	case <-ready:
		return nil
	case <-ctx.Done():
		err = ctx.Err()
	case <-stop:
		err = mq.ErrClosed
	}
	r.mu.Lock()
	select {
	case <-ready:
		// A place came as the wait ended, and goes to the next waiter.
		r.mu.Unlock()
		r.give()
		return err
	default:
	}
	r.waiters = slices.DeleteFunc(r.waiters, func(w chan struct{}) bool { return w == ready })
	r.mu.Unlock()
	return err
}

// give hands a place back: to the first waiter, or, while somebody is in the
// line, to the free places, waking the first in the line.
func (r *room) give() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.grow()
	r.free++
	r.gives++
	if r.wakeNext() || len(r.line) > 0 {
		return
	}
	r.handOut()
}

// wakeNext wakes the first publication in the line that has no wake-up
// waiting, reporting whether there was one. The caller holds r.mu.
func (r *room) wakeNext() bool {
	for _, e := range r.line {
		if !e.woken {
			e.woken = true
			e.wake <- struct{}{}
			return true
		}
	}
	return false
}

// refused puts a publication the client refused, which keeps its place, in
// the line: at the end the first time, and back at the front after a try,
// woken at once if a place came back during that try. It returns the
// publication's place in the line.
//
// A place that came back between the client's first refusal and this found
// nobody in the line to wake, and waits in free, so a first refusal that finds
// a free place wakes the line too.
func (r *room) refused(e *retrier) *retrier {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e == nil {
		e = &retrier{wake: make(chan struct{}, 1), inLine: true}
		r.line = append(r.line, e)
		if r.free > 0 {
			r.wakeNext()
		}
		return e
	}
	r.line = slices.Insert(r.line, 0, e)
	e.inLine = true
	if r.gives != e.gives && !e.woken {
		e.woken = true
		e.wake <- struct{}{}
	}
	return e
}

// retry waits for e's next try: a place came back and woke it, or d passed
// while it was first in the line. It then takes e out of the line for the
// try, and the caller puts it back if the client refuses it again
// ([room.refused]) or ends its place in the line ([room.leave]). retry ends
// early when ctx ends or stop is closed, which it reports as ctx's error or
// ErrClosed, and the caller then leaves the line.
func (r *room) retry(ctx context.Context, stop <-chan struct{}, e *retrier, d time.Duration) error {
	for {
		t := time.NewTimer(d)
		var err error
		woken := false
		select {
		case <-e.wake:
			woken = true
		case <-t.C:
		case <-ctx.Done():
			err = ctx.Err()
		case <-stop:
			err = mq.ErrClosed
		}
		t.Stop()
		if err != nil {
			return err
		}
		r.mu.Lock()
		if woken || r.line[0] == e {
			r.line = slices.DeleteFunc(r.line, func(o *retrier) bool { return o == e })
			e.inLine = false
			if e.woken && !woken {
				// The pause ended as a wake-up came: this try is its.
				<-e.wake
			}
			e.woken = false
			e.gives = r.gives
			r.mu.Unlock()
			return nil
		}
		r.mu.Unlock()
	}
}

// leave ends e's place in the line, if there is one, once the client has taken
// its publication or it has given up, passing on a wake-up it had not used,
// and when the line is empty hands the free places to the waiters. The place
// in the room stays with the publication.
func (r *room) leave(e *retrier) {
	if r == nil || e == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if e.inLine {
		r.line = slices.DeleteFunc(r.line, func(o *retrier) bool { return o == e })
		e.inLine = false
		if e.woken {
			e.woken = false
			r.wakeNext()
		}
	}
	if len(r.line) == 0 {
		r.handOut()
	}
}

// abandoned records that a synchronous Send returned on its context. The
// client may still hold its request, out of the room's count since the Send's
// place went back, and frees that room only when it answers the request,
// which no place coming back says ([refusedQueue]).
func (r *room) abandoned() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.abandonedAt = time.Now()
	r.mu.Unlock()
}

// abandonedLately reports whether a synchronous Send returned on its context
// within abandonedFor.
func (r *room) abandonedLately() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return time.Since(r.abandonedAt) < abandonedFor
}

// abandonedFor is how long after a synchronous Send returned on its context a
// full queue is taken for room the client is still holding for publications
// whose callers gave up on them ([sink.waitOut]): long enough to cover the
// gaps between abandoned Sends while callers keep giving up on their
// deadlines, which the hunter's probe of deadlines under a few milliseconds
// let through about 10% more publications with than with 50 ms.
const abandonedFor = time.Second

// roomFor is how many places for each partition the room of a producer holds
// under cfg: none without BlockIfQueueFull, and otherwise MaxPendingMessages,
// or v0.21's own default of 1000 when that is zero.
func roomFor(cfg Config) int {
	switch {
	case !cfg.BlockIfQueueFull:
		return 0
	case cfg.MaxPendingMessages > 0:
		return cfg.MaxPendingMessages
	}
	return defaultMaxPendingMessages
}

// defaultMaxPendingMessages is pulsar-client-go v0.21's MaxPendingMessages
// when the option is zero (newPartitionProducer).
const defaultMaxPendingMessages = 1000

// The client's refusals of a publication for want of room ([refusalOf]),
// which under BlockIfQueueFull the driver waits out ([sink.waitOut]).
const (
	notRefused int32 = iota
	// refusedQueue is a full queue: the partition the client picked holds
	// MaxPendingMessages, which happens when one partition takes more than
	// its share, or the queue still holds publications whose callers gave up
	// on them (a Send that returns on its context leaves its request queued,
	// out of the room's count, [room.abandoned]).
	refusedQueue
	// refusedMemory is a full memory buffer (MemoryLimitBytes), which every
	// publication of the client shares.
	refusedMemory
)

// refusalOf is which refusal for want of room err is, if any.
func refusalOf(err error) int32 {
	if err == nil {
		return notRefused
	}
	r, ok := ResultOf(err)
	switch {
	case !ok:
		return notRefused
	case r == pulsar.ClientMemoryBufferIsFull:
		return refusedMemory
	case r == pulsar.ProducerQueueIsFull:
		return refusedQueue
	}
	return notRefused
}

// roomRetry is the first pause before a refused publication is handed to the
// client again when no place has come back to wake it first, which doubles up
// to roomRetryMax. The client says nothing when room comes back, so the pause
// is for room it frees without a place coming back: in another producer's
// share of the memory buffer, or as it answers publications whose callers gave
// up on them.
const (
	roomRetry    = time.Millisecond
	roomRetryMax = 50 * time.Millisecond
)

// pause waits d before a publication that gave its place back is handed to
// the client again ([sink.waitOut]), until ctx ends or stop is closed, which it
// reports as ctx's error or ErrClosed.
func pause(ctx context.Context, stop <-chan struct{}, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-stop:
		return mq.ErrClosed
	}
}

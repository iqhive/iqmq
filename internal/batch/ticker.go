package batch

import (
	"sync"
	"time"

	"bitbucket.org/iqhive/iqmq/internal/clock"
)

// Ticker is the shared linger clock of the SharedTicker kind: one timer
// and one goroutine for any number of batchers.
//
// It ticks on a fixed grid anchored at the instant it was created, so a
// batch's deadline is the first grid point at or after its linger
// deadline — at most one period late, and never early. Registered
// batchers are woken on every tick; each decides for itself whether it is
// ready, so a tick costs one non-blocking channel send per registered
// batcher and nothing else.
type Ticker struct {
	clk    clock.Clock
	period time.Duration
	epoch  time.Time

	mu      sync.Mutex
	waiters map[chan time.Time]struct{}
	closed  bool

	done chan struct{}
	// stopped is closed when the ticking goroutine has returned, so Close
	// leaves no goroutine behind (the leak checks in the engine's tests
	// depend on it).
	stopped chan struct{}
}

// NewTicker starts a ticker on clk with the given period (a non-positive
// period means one millisecond). Close it when the publishers that share
// it are gone.
func NewTicker(clk clock.Clock, period time.Duration) *Ticker {
	if clk == nil {
		clk = clock.Real{}
	}
	if period <= 0 {
		period = time.Millisecond
	}
	t := &Ticker{
		clk:     clk,
		period:  period,
		epoch:   clk.Now(),
		waiters: make(map[chan time.Time]struct{}),
		done:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
	// The timer is armed here, not in the goroutine, so that a caller that
	// counts timers (or advances a fake clock) sees it immediately.
	timer := clk.NewTimer(period)
	go t.run(timer)
	return t
}

// Period returns the ticker's period.
func (t *Ticker) Period() time.Duration { return t.period }

// run wakes every registered batcher on each tick.
func (t *Ticker) run(timer clock.Timer) {
	defer close(t.stopped)
	defer timer.Stop()
	for {
		select {
		case now := <-timer.C():
			t.broadcast(now)
			timer.Reset(t.period)
		case <-t.done:
			return
		}
	}
}

// broadcast delivers a wake-up to every registered batcher without ever
// blocking: a batcher that has not drained its previous wake-up is
// already awake.
func (t *Ticker) broadcast(now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for ch := range t.waiters {
		select {
		case ch <- now:
		default:
		}
	}
}

// deadlineFor rounds a linger deadline up to the next tick of the grid. A
// deadline at or before the epoch lands on the epoch itself.
func (t *Ticker) deadlineFor(want time.Time) time.Time {
	d := want.Sub(t.epoch)
	if d <= 0 {
		return t.epoch
	}
	n := d / t.period
	if d%t.period != 0 {
		n++
	}
	return t.epoch.Add(n * t.period)
}

func (t *Ticker) register(ch chan time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.closed {
		t.waiters[ch] = struct{}{}
	}
}

func (t *Ticker) unregister(ch chan time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.waiters, ch)
}

// Waiters returns the number of registered batchers. It exists for tests
// and for the timers/op accounting in benchmarks.
func (t *Ticker) Waiters() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.waiters)
}

// Close stops the ticker and returns once its goroutine has finished. It
// is idempotent.
func (t *Ticker) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		<-t.stopped
		return nil
	}
	t.closed = true
	t.waiters = map[chan time.Time]struct{}{}
	close(t.done)
	t.mu.Unlock()
	<-t.stopped
	return nil
}

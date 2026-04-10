package iqmq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"bitbucket.org/iqhive/iqmq/internal/clock"
)

// TestPendingBoundNeverLosesAWakeUp is the property BackpressureBlock rests on
// and the one hack/PLAYBOOK.md's recurring defect keeps breaking: a place
// given back has to reach whoever is parked waiting for one.
//
// It is an internal test because the invariant it pins is not observable from
// outside: "if any publication is parked on the current wake channel then the
// bound knows somebody is waiting". A release consults that flag before it
// broadcasts, so a flag that says nobody is waiting while somebody is parked
// is a place returned to a buffer with a publication asleep on it — and the
// publication then sleeps until its context, its timeout or the publisher's
// Close, none of which a caller under BackpressureBlock has asked for.
//
// The exercise is the smallest one that produces the interleaving: a bound of
// ONE, so every publication after the first parks, and eight goroutines each
// taking a place and giving it straight back, so the bound is broadcast on and
// re-registered thousands of times. What used to happen is the specification: a publication woken by a broadcast decremented a
// counter the broadcast had already zeroed, ate the registration of a
// publication that had parked since, and then found room and left without
// re-registering — so the next release saw a count of zero and broadcast
// nothing.
//
// The failure mode is a hang, so the wait is bounded here and the bound's own
// state is reported: a test that only timed out would say nothing about why.
func TestPendingBoundNeverLosesAWakeUp(t *testing.T) {
	t.Parallel()
	const (
		rounds  = 40
		writers = 8
		perRun  = 25
	)
	for round := range rounds {
		b := newPendingBound(PublisherConfig{
			Backpressure:       BackpressureBlock,
			MaxPendingMessages: 1,
		}, clock.Real{})

		var (
			writersWG sync.WaitGroup
			releaser  sync.WaitGroup
			done      = make(chan struct{})
			ok        atomic.Int64
			bad       atomic.Int64
		)
		// The place is given back by ANOTHER goroutine, which is the whole
		// point: in a publisher the place is released by the completion, so
		// every writer can be parked at once and only this goroutine can
		// free anything. A writer that released its own place would always
		// leave one runnable goroutine and the wake cohort would never form.
		//
		// The channel holds every placement, so a writer never blocks on the
		// hand-over and the only thing it can block on is the bound itself.
		taken := make(chan struct{}, writers*perRun)
		releaser.Add(1)
		go func() {
			defer releaser.Done()
			for range taken {
				b.release(1)
			}
		}()
		for range writers {
			writersWG.Add(1)
			go func() {
				defer writersWG.Done()
				for range perRun {
					if _, err := b.admit(context.Background(), 1, timeoutBound{}); err != nil {
						bad.Add(1)
						return
					}
					ok.Add(1)
					taken <- struct{}{}
				}
			}()
		}
		go func() {
			writersWG.Wait()
			close(taken)
			releaser.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(10 * time.Second):
			b.mu.Lock()
			waiting, msgs := b.waiting, b.msgs
			b.mu.Unlock()
			t.Fatalf("round %d: %d of %d placements completed and the rest are parked for ever: "+
				"the bound holds %d message(s) of %d and says waiting=%v, so a release found nobody to wake while publications were asleep on the channel it did not close",
				round, ok.Load(), writers*perRun, msgs, b.maxMsgs, waiting)
		}
		if got := bad.Load(); got != 0 {
			t.Fatalf("round %d: %d placements were refused; BackpressureBlock with a background context refuses nothing", round, got)
		}
		if got := ok.Load(); got != writers*perRun {
			t.Fatalf("round %d: %d placements completed, want %d", round, got, writers*perRun)
		}
		if b.msgs != 0 || b.bytes != 0 {
			t.Fatalf("round %d: the bound holds %d message(s) and %d byte(s) after every placement was released",
				round, b.msgs, b.bytes)
		}
	}
}

// TestPendingBoundCloseReleasesEveryParkedPublication is the specification's "Close
// unblocks blocked operations" for the one operation of a publisher that
// really parks, and it is the other half of the invariant above: the flag a
// release consults is the same one close consults, so a lost registration
// would strand a publication through a shutdown too.
func TestPendingBoundCloseReleasesEveryParkedPublication(t *testing.T) {
	t.Parallel()
	b := newPendingBound(PublisherConfig{
		Backpressure:       BackpressureBlock,
		MaxPendingMessages: 1,
	}, clock.Real{})
	if _, err := b.admit(context.Background(), 1, timeoutBound{}); err != nil {
		t.Fatalf("the first placement in an empty bound: %v", err)
	}
	const parked = 4
	var wg sync.WaitGroup
	errs := make([]error, parked)
	for i := range parked {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = b.admit(context.Background(), 1, timeoutBound{})
		}(i)
	}
	// Wait until the bound really has registered somebody, so the close below
	// is a close against parked publications and not a race with their
	// arrival.
	deadline := time.Now().Add(5 * time.Second)
	for {
		b.mu.Lock()
		waiting := b.waiting
		b.mu.Unlock()
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no publication registered as waiting on a full bound")
		}
		time.Sleep(time.Millisecond)
	}
	b.close()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("a parked publication outlived the bound's close: the specification makes Close unblock a blocked operation")
	}
	for i, err := range errs {
		if !errors.Is(err, ErrClosed) {
			t.Errorf("parked publication %d reported %v, want ErrClosed", i, err)
		}
	}
}

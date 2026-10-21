package iqmq

import (
	"context"
	"sync"
	"time"

	"github.com/iqhive/iqmq/internal/clock"
	"github.com/iqhive/iqmq/internal/errcode"
)

// BackpressureMode is what a publisher does when its own bounded buffer is
// full.
//
// the rule is that "every internal buffer is bounded by default (count
// and bytes)", and the mode names what happens at the bound. The buffer a
// publisher owns is the set of publications it has admitted and not yet
// completed: the asynchronous publications outstanding at the driver and the
// messages waiting in the batching buffer. A synchronous Publish holds no
// client-side buffer — it blocks in the driver until the broker answers — so
// it is not governed by a mode at all.
//
// The zero value is BackpressureBlock, which the specification names as the default.
type BackpressureMode uint8

// The backpressure modes.
const (
	// BackpressureBlock waits for room, and is the zero value and the
	// default. The wait ends when room appears, when the caller's context
	// is done, or when PublisherConfig.Timeout has passed.
	BackpressureBlock BackpressureMode = iota
	// BackpressureReject refuses the publication with ErrBackpressure
	// immediately. It refuses before anything is allocated for the
	// publication and before the driver is told anything.
	BackpressureReject
	// BackpressureDropNewest discards the arriving publication, counts it
	// in PublisherStats.Drops and reports ErrBackpressure. It is opt-in and
	// never "reliable": a dropped publication never reaches the broker.
	BackpressureDropNewest
	// BackpressureDropOldest discards the oldest publication still waiting
	// in the batching buffer and admits the arriving one. The discarded
	// publication's callback runs exactly once, with ErrBackpressure, and
	// is counted in PublisherStats.Drops.
	//
	// A publisher with nothing queued has nothing to discard — a
	// publication already handed to the driver cannot be recalled — so the
	// mode then behaves as BackpressureReject, which is the same
	// degradation the engine applies to an overflow policy that would have
	// to park.
	BackpressureDropOldest

	backpressureModeCount
)

var backpressureModeNames = [backpressureModeCount]string{"block", "reject", "dropnewest", "dropoldest"}

// String returns the mode's lower-case name, which is also the name the specification
// uses for the overflow dimension.
func (b BackpressureMode) String() string {
	if b < backpressureModeCount {
		return backpressureModeNames[b]
	}
	return "BackpressureMode(?)"
}

// Valid reports whether b names a backpressure mode.
func (b BackpressureMode) Valid() bool { return b < backpressureModeCount }

// Reliable reports whether the mode never loses a publication it admitted.
// Blocking and rejecting are reliable — the caller is either served or told —
// and the two drop modes are not, which is why the specification makes them opt-in,
// counted and never a default.
func (b BackpressureMode) Reliable() bool {
	return b == BackpressureBlock || b == BackpressureReject
}

// DefaultMaxPendingMessages is the bound a publisher applies to its own
// buffer when PublisherConfig.MaxPendingMessages is zero.
//
// The specification requires the buffer to be bounded *by default*, so zero cannot mean
// unbounded; it means this. It is completion.DefaultCapacity, because the
// completion table is the other half of the same bound and a publisher whose
// two halves disagreed would report ErrFull from the table before its own
// mode was ever consulted.
const DefaultMaxPendingMessages = 1024

// errDropped is what a dropped publication reports. The specification names
// ErrBackpressure for a full client-side buffer and no separate sentinel for
// a drop, so a drop is an ErrBackpressure that PublisherStats.Drops counts.
var errDropped = errcode.New(errcode.Backpressure, "iqmq: publication dropped by the publisher's backpressure mode")

// errPublishTimeout is what a core-side wait that ran out of
// PublisherConfig.Timeout reports. Its code is CodeDeadlineExceeded, so
// errors.Is(err, context.DeadlineExceeded) holds for it the way the specification
// requires of every deadline.
var errPublishTimeout = errcode.New(errcode.DeadlineExceeded, "iqmq: publication exceeded PublisherConfig.Timeout")

// pendingBound is the publisher's bounded buffer accounting: how many
// publications it has admitted and not completed, and how many bytes they
// carry.
//
// It is consulted before anything is allocated for a publication, which is
// what makes BackpressureReject reject "before it allocates": the counter is
// the first thing a publication meets and the pooled state, the completion
// slot and the batch entry all come after it.
type pendingBound struct {
	mode     BackpressureMode
	maxMsgs  int
	maxBytes int64
	clock    clock.Clock

	mu    sync.Mutex
	msgs  int
	bytes int64
	// closing is set by close, which is the publisher's StopAccept step. A
	// publication parked for room re-reads it after every wake-up, so
	// the specification's "Close unblocks blocked operations" reaches the one operation
	// of the publisher that really parks.
	closing bool
	// waiting says whether any publication has registered on the CURRENT
	// wake channel. wake is closed to release them all and replaced; a
	// release with nobody registered touches neither, so an uncontended
	// release allocates nothing.
	//
	// It is deliberately a flag and not a COUNT of parked publications, and
	// that is the whole. A count has to be decremented
	// by somebody, and the only place a parked publication can do that is
	// after its wake-up — by which time the broadcast that woke it has
	// already cleared the count, so the decrement lands on the registration
	// of a publication that parked since. A publication that then found room
	// and left took that registration with it, and the next release saw a
	// count of zero, broadcast nothing, and left whoever was still asleep on
	// the channel asleep until its context, its timeout or the publisher's
	// Close — which is none of the three things a caller under
	// BackpressureBlock asked for.
	//
	// A flag cannot express that state. It is set under the lock in the same
	// critical section that snapshots the channel, and it is cleared only by
	// the two functions that close that channel, in the same critical
	// section — so "somebody is parked on b.wake" implies "b.waiting", with
	// no window between the two. What it gives up is precision in the other
	// direction: a publication that registered and then gave up on its
	// context or its timeout, or one that woke and found room, leaves the
	// flag set, and the next release therefore broadcasts to nobody. That
	// costs one channel allocation on a release that followed a wait, never
	// on the uncontended path, and it self-corrects because the broadcast
	// clears the flag.
	waiting bool
	wake    chan struct{}
	// head and tail are the queued publications in arrival order, which is
	// what BackpressureDropOldest discards from. A publication is linked
	// when it enters the batching buffer and unlinked when the flusher takes
	// it or when it is dropped, so the list holds exactly the publications
	// that can still be recalled.
	head, tail *asyncPub
}

// newPendingBound builds the bound from a resolved publisher configuration.
func newPendingBound(cfg PublisherConfig, clk clock.Clock) *pendingBound {
	b := &pendingBound{
		mode:     cfg.Backpressure,
		maxMsgs:  cfg.MaxPendingMessages,
		maxBytes: cfg.MaxPendingBytes,
		clock:    clk,
		wake:     make(chan struct{}),
	}
	if b.maxMsgs <= 0 {
		b.maxMsgs = DefaultMaxPendingMessages
	}
	return b
}

// fits reports whether one more publication of n bytes stays inside the
// bound. b.mu held.
//
// An empty buffer always fits one publication, whatever its size: a byte
// bound smaller than a single message would otherwise park a publisher for
// ever under BackpressureBlock, waiting for room that nothing can create.
// It is the same rule internal/batch applies to an item larger than MaxBytes.
func (b *pendingBound) fits(n int64) bool {
	if b.msgs == 0 {
		return true
	}
	if b.maxMsgs > 0 && b.msgs+1 > b.maxMsgs {
		return false
	}
	return b.maxBytes <= 0 || b.bytes+n <= b.maxBytes
}

// admit takes one place of n bytes in the buffer, applying the mode when
// there is none.
//
// It is called OUTSIDE the publication gate, and has to be: under
// BackpressureBlock it parks, and internal/lifecycle's rule is that the
// section between Enter and Leave must not block — a publication that parked
// inside the gate would deadlock against its own Close, which waits for the
// gate to drain before it frees anything. The ticket is taken afterwards, for
// the placement alone, and a publication the gate then refuses gives its
// place back.
//
// It returns nil when the publication may proceed, and the mode's error
// otherwise: ErrBackpressure for a refusal or a drop, ctx.Err() for a
// cancelled wait, and ErrDeadlineExceeded's code for a wait that ran out of
// PublisherConfig.Timeout. dropped reports whether the refusal was a drop,
// which is what the caller counts.
func (b *pendingBound) admit(ctx context.Context, n int64, timeout timeoutBound) (dropped bool, err error) {
	for {
		b.mu.Lock()
		if b.closing {
			b.mu.Unlock()
			return false, ErrClosed
		}
		if b.fits(n) {
			b.msgs++
			b.bytes += n
			b.mu.Unlock()
			return false, nil
		}
		switch b.mode {
		case BackpressureReject:
			b.mu.Unlock()
			return false, ErrBackpressure
		case BackpressureDropNewest:
			b.mu.Unlock()
			return true, errDropped
		case BackpressureDropOldest:
			victim := b.unlinkOldestLocked()
			if victim != nil {
				// The victim's place is given back here and now, not by its
				// completion: under CallbackDeferred that runs later on the
				// completion goroutine, and the loop would go on discarding
				// the queue while it waited (pendingBound.releaseOf).
				b.releaseLocked(victim.bytes)
			}
			b.mu.Unlock()
			if victim == nil {
				// Nothing is queued: everything admitted is already at the
				// driver and cannot be recalled. The opt-in drop
				// degrades to a refusal rather than to a park the caller
				// did not ask for.
				return false, ErrBackpressure
			}
			// The callback runs outside the lock: no user code may run
			// under an internal lock, which is the rule for Release and
			// the same discipline for a completion.
			victim.drop()
		case BackpressureBlock:
			// Registering and snapshotting the channel happen in ONE
			// critical section, so a release cannot broadcast between them:
			// either it closes the channel this publication is about to park
			// on, or it has not run yet and will see the flag.
			b.waiting = true
			ch := b.wake
			b.mu.Unlock()
			if err := b.park(ctx, ch, timeout); err != nil {
				// The registration is left set rather than withdrawn, which
				// is what makes the accounting safe: see pendingBound.waiting.
				return false, err
			}
			// And the wake-up is not "there is room for me" — it is "the
			// buffer changed". The loop re-reads the bound under the lock,
			// which is where a publication that lost the room to somebody
			// else registers again.
		default:
			b.mu.Unlock()
			return false, ErrBackpressure
		}
	}
}

// park waits for room, for the caller's context, or for the publisher's
// timeout. The timer is created only when a publication actually blocks, so
// The specification rule 3's "no timer per message" holds for every publication that is
// admitted.
func (b *pendingBound) park(ctx context.Context, ch <-chan struct{}, timeout timeoutBound) error {
	if !timeout.set {
		select {
		case <-ch:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	t := b.clock.NewTimer(timeout.left(b.clock))
	defer t.Stop()
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C():
		return errPublishTimeout
	}
}

// release gives one place of n bytes back and wakes everybody parked.
//
// It wakes ALL of them rather than handing the place to one, because the
// place is not the only thing they are waiting on: the bound is a count and a
// byte total, and which parked publication now fits depends on its size. The
// woken publications re-read the bound and the ones that still do not fit
// register again.
func (b *pendingBound) release(n int64) {
	b.mu.Lock()
	b.releaseLocked(n)
	b.mu.Unlock()
}

// releaseOf is release for an admitted publication's completion: the place
// of one BackpressureDropOldest discarded was given back by the drop.
func (b *pendingBound) releaseOf(ap *asyncPub, n int64) {
	b.mu.Lock()
	if !ap.dropped {
		b.releaseLocked(n)
	}
	b.mu.Unlock()
}

// releaseLocked is release with b.mu held.
func (b *pendingBound) releaseLocked(n int64) {
	b.msgs--
	b.bytes -= n
	if b.msgs < 0 {
		b.msgs = 0
	}
	if b.bytes < 0 {
		b.bytes = 0
	}
	if b.waiting {
		close(b.wake)
		b.wake = make(chan struct{})
		b.waiting = false
	}
}

// close bars the buffer and releases every parked publication with ErrClosed,
// which is the specification's "Close unblocks blocked operations".
//
// It is the publisher's StopAccept step, so it runs as soon as the phase that
// stops admitting publications is entered — before any step that waits — and
// a publication parked for room is therefore released at the earliest point a
// shutdown has anything to say to it.
func (b *pendingBound) close() {
	b.mu.Lock()
	b.closing = true
	if b.waiting {
		close(b.wake)
		b.wake = make(chan struct{})
		b.waiting = false
	}
	b.mu.Unlock()
}

// link adds ap to the queued list, where BackpressureDropOldest can still
// reach it.
func (b *pendingBound) link(ap *asyncPub) {
	b.mu.Lock()
	ap.prev, ap.next = b.tail, nil
	if b.tail != nil {
		b.tail.next = ap
	} else {
		b.head = ap
	}
	b.tail = ap
	ap.queued = true
	b.mu.Unlock()
}

// unlink takes ap out of the queued list and reports whether it had already
// been dropped, in which case the caller must not publish it: the dropped
// publication's callback has already run.
//
// Dropping marks under the same lock that unlinks, so the flusher and a
// dropping publisher cannot both take one message: whichever reaches the lock
// first owns it.
func (b *pendingBound) unlink(ap *asyncPub) (wasDropped bool) {
	b.mu.Lock()
	b.unlinkLocked(ap)
	wasDropped = ap.dropped
	b.mu.Unlock()
	return wasDropped
}

// unlinkLocked removes ap from the queued list. b.mu held.
func (b *pendingBound) unlinkLocked(ap *asyncPub) {
	if !ap.queued {
		return
	}
	if ap.prev != nil {
		ap.prev.next = ap.next
	} else {
		b.head = ap.next
	}
	if ap.next != nil {
		ap.next.prev = ap.prev
	} else {
		b.tail = ap.prev
	}
	ap.prev, ap.next, ap.queued = nil, nil, false
}

// unlinkOldestLocked takes the oldest queued publication out of the list and
// marks it dropped. b.mu held; the caller completes it outside the lock.
func (b *pendingBound) unlinkOldestLocked() *asyncPub {
	ap := b.head
	if ap == nil {
		return nil
	}
	b.unlinkLocked(ap)
	ap.dropped = true
	return ap
}

// timeoutBound is PublisherConfig.Timeout as one publication sees it: the
// instant its core-side waits must end, or nothing when the publisher has no
// timeout.
//
// It is a deadline and not a fresh duration per wait, so a publication that
// parks twice — once for buffer room and once for its batch to leave — is
// bounded once in total rather than once per wait. It carries no context: a
// per-message context.WithTimeout is what rule 4 forbids, and this
// bounds only the waits the core itself performs.
type timeoutBound struct {
	at  time.Time
	set bool
}

// newTimeoutBound starts a bound of d from now, or an unset one when d is not
// positive. It reads the clock only when there is a timeout to measure, which
// is rule 4's rule for the clock.
func newTimeoutBound(clk clock.Clock, d time.Duration) timeoutBound {
	if d <= 0 {
		return timeoutBound{}
	}
	return timeoutBound{at: clk.Now().Add(d), set: true}
}

// left is how much of the bound remains. A bound that has already passed
// leaves a single nanosecond rather than zero, so that a wait always reaches
// its timer branch instead of parking on a timer that will never fire.
func (t timeoutBound) left(clk clock.Clock) time.Duration {
	d := t.at.Sub(clk.Now())
	if d <= 0 {
		return 1
	}
	return d
}

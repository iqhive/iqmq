package confirm

import (
	"sync"

	"github.com/iqhive/iqmq/internal/errcode"
)

// Tracker is one confirm-mode channel's outstanding publications.
//
// It is the MECHANISM of All three kinds drive the same tracker
// and differ only in when the publisher waits. Everything the AMQP 0-9-1
// confirm protocol does that is easy to get wrong lives here and nowhere else:
// the monotonic delivery tag, the cumulative `multiple` flag, out-of-order
// confirms, the `basic.return` that precedes an ack and turns it into a
// rejection, and the channel close that turns every publication in flight into
// an ambiguous outcome rather than a failed one.
//
// One tracker belongs to one channel. Delivery tags restart at 1 on a new
// channel, so a tracker is never reused across a reconnection: the driver
// detaches the old one, which resolves what it held, and builds a new one.
// [Tracker.Detach] is terminal and every later confirm on a detached tracker
// is discarded — which is the publisher-side half of the same rule
// `driverkit.TokenTable`'s generation enforces on the consumer side. Without
// it a confirm frame that was already in the socket when the channel died
// would resolve the publication that took its tag on the NEW channel, which is
// one publication being told another's outcome.
//
// Every method is safe for concurrent use: a publisher submits from the
// caller's goroutine and the client library delivers confirms on its own, so
// the two really do race. The lock is held over bookkeeping only and never
// over a callback.
type Tracker struct {
	mu sync.Mutex
	// next is the tag the next publication will be given. RabbitMQ's first
	// delivery tag on a confirm channel is 1, so this starts at 1 and a tag
	// of 0 is never valid.
	next uint64
	// live are the outstanding publications, keyed by tag. A map rather than
	// a ring because confirms arrive out of order and cumulatively, so the
	// structure has to answer "every tag at or below n" and "this one tag"
	// equally well; the window bounds its size.
	live map[uint64]*entry
	// byToken indexes the outstanding publications a `basic.return` can name,
	// which is only those published with a correlation token.
	byToken map[string]*entry
	// lowest is the smallest outstanding tag, which is what makes a
	// cumulative confirm O(resolved) rather than O(tags ever published). A
	// `multiple` ack naming tag 1,000,000 on a window of 64 must not walk a
	// million integers.
	lowest uint64
	// detached is set by Detach and is terminal.
	detached bool
	// resolved counts terminal outcomes, for the model and the simulation.
	resolved uint64
	// ambiguous counts the outcomes Detach produced, which is the specification's
	// "ambiguity accuracy" as an observation rather than a bound.
	ambiguous uint64
}

// entry is one outstanding publication.
type entry struct {
	tag      uint64
	token    string
	returned bool
}

// NewTracker returns a tracker for one channel, sized for hint outstanding
// publications. A non-positive hint uses [DefaultWindow].
func NewTracker(hint int) *Tracker {
	if hint <= 0 {
		hint = DefaultWindow
	}
	return &Tracker{
		next:    1,
		live:    make(map[uint64]*entry, hint),
		byToken: make(map[string]*entry, hint),
		lowest:  1,
	}
}

// Submit records a publication and returns the delivery tag the broker will
// confirm it under.
//
// The tag is assigned HERE rather than by the caller, and that is the one
// ordering rule the protocol gives no second chance on: the broker numbers
// publications in the order the frames reach it, so the tag has to be taken
// under the same lock that admits the publication, and the caller has to write
// the frame while holding whatever serialises its channel. A driver that
// allocated a tag, released the lock and then wrote would publish tag 8 before
// tag 7 and mis-attribute both confirms.
//
// token is the correlation token a `basic.return` will be matched by, and is
// empty for a strategy whose [Strategy.NeedsCorrelation] is false.
func (t *Tracker) Submit(token string) (uint64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.detached {
		return 0, ErrDetached
	}
	tag := t.next
	t.next++
	e := &entry{tag: tag, token: token}
	t.live[tag] = e
	if token != "" {
		if _, dup := t.byToken[token]; dup {
			// Two outstanding publications under one token would make a
			// return attributable to either, which is the very thing the
			// token exists to prevent. Refusing is the only honest answer,
			// and the driver's token is a counter so it cannot happen by
			// accident.
			delete(t.live, tag)
			t.next--
			return 0, ErrDuplicateToken
		}
		t.byToken[token] = e
	}
	return tag, nil
}

// Unsubmit takes back the publication [Tracker.Submit] most recently issued,
// for a publish that was refused before its frame reached the socket.
//
// It exists because the CLIENT LIBRARY does the same thing and the two counters
// have to stay aligned. amqp091-go increments its own published counter before
// it writes the frame and calls `confirms.unpublish()` when the write fails,
// so a driver that assigned a tag and then failed to publish would be one
// ahead of the library for ever afterwards: the library would give the next
// successful publication tag N and the driver would believe it was N+1, and
// every confirm from then on would resolve the wrong publication. That is a
// silent cross-message swap rather than a crash, which is the failure mode
// the lesson is about -- what reaches a driver is what the library hands
// over, not what the protocol says.
//
// It refuses anything but the highest outstanding tag, because that is the
// only case the library can produce and the only one that keeps the tag
// sequence gapless: a publication in the middle of the window has already
// been written, so it cannot be taken back.
//
// A publication whose write FAILED is a different case and must not come here:
// the frame may have reached the broker, so the outcome is ambiguous and
// [Tracker.Detach] is the answer.
func (t *Tracker) Unsubmit(tag uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.detached {
		return ErrDetached
	}
	if tag == 0 || tag != t.next-1 {
		return errNotLastSubmitted
	}
	e, ok := t.live[tag]
	if !ok {
		return errNotLastSubmitted
	}
	delete(t.live, tag)
	if e.token != "" {
		delete(t.byToken, e.token)
	}
	t.next--
	if t.lowest > t.next {
		t.lowest = t.next
	}
	return nil
}

// Ack applies a `basic.ack` and returns the publications it resolved.
//
// With multiple set it resolves this tag and every lower outstanding tag,
// which is what the protocol's cumulative form means. A publication that was
// returned first resolves as [Rejected]: the broker took it and could not
// route it, which the specification makes a rejection and not a success.
//
// The slice is freshly allocated and the caller owns it. That is deliberate
// even though this is a hot path: the outcomes are handed to callbacks that may
// outlive the call, and a shared buffer would be the cross-message swap
// The specification records for pooled option sets.
func (t *Tracker) Ack(tag uint64, multiple bool) []Outcome {
	return t.resolve(tag, multiple, Confirmed, false)
}

// Nack applies a `basic.nack` and returns the publications it resolved, all as
// [Rejected].
//
// RabbitMQ sends a nack when it could not take responsibility for a
// publication — an internal error, a queue that could not be written. It is a
// definite negative: the message is not in a queue, so a caller may retry it
// without duplicating anything, which is exactly what distinguishes it from
// [Ambiguous].
func (t *Tracker) Nack(tag uint64, multiple, requeue bool) []Outcome {
	return t.resolve(tag, multiple, Rejected, requeue)
}

// resolve is the one implementation of both confirm forms.
func (t *Tracker) resolve(tag uint64, multiple bool, result Result, requeue bool) []Outcome {
	t.mu.Lock()
	if t.detached || tag == 0 || tag >= t.next {
		// A tag the tracker never issued is a confirm from a channel this
		// tracker does not own — a frame still in the socket when the
		// channel died, or a broker bug. Discarding it is the publisher-side
		// staleness guard; applying it would resolve a publication that took
		// the tag later.
		t.mu.Unlock()
		return nil
	}
	var out []Outcome
	if !multiple {
		e, ok := t.live[tag]
		if !ok {
			t.mu.Unlock()
			return nil
		}
		out = append(out, t.takeLocked(e, result, requeue))
		t.advanceLocked()
		t.mu.Unlock()
		return out
	}
	// The cumulative form. Walking from the lowest outstanding tag is what
	// keeps this proportional to what it resolves: the window bounds the
	// number of live tags, and `lowest` is never behind the first of them.
	for n := t.lowest; n <= tag; n++ {
		e, ok := t.live[n]
		if !ok {
			continue
		}
		out = append(out, t.takeLocked(e, result, requeue))
	}
	t.advanceLocked()
	t.mu.Unlock()
	return out
}

// takeLocked removes one entry and builds its outcome.
func (t *Tracker) takeLocked(e *entry, result Result, requeue bool) Outcome {
	delete(t.live, e.tag)
	if e.token != "" {
		delete(t.byToken, e.token)
	}
	t.resolved++
	if e.returned {
		// A returned publication is Rejected whatever the confirm said: the
		// ack that follows a return means "I took it and threw it away".
		result, requeue = Rejected, false
	}
	return Outcome{Tag: e.tag, Token: e.token, Result: result, Requeue: requeue, Returned: e.returned}
}

// advanceLocked moves `lowest` up past the tags that are no longer
// outstanding.
func (t *Tracker) advanceLocked() {
	for t.lowest < t.next {
		if _, ok := t.live[t.lowest]; ok {
			return
		}
		t.lowest++
	}
}

// Returned records a `basic.return`: the broker accepted the publication and
// no queue was bound for its routing key, so the `basic.ack` that follows
// resolves it as [Rejected].
//
// It is matched by the correlation token because the frame carries no delivery
// tag. It reports false for a token the tracker does not hold, which is a
// return for a publication that has already been confirmed — impossible on a
// well-behaved broker, since the return precedes the ack, and worth reporting
// rather than guessing at.
//
// The token is empty for [PerMessage], whose single outstanding publication is
// the only candidate; [Tracker.ReturnedOnly] is that case.
func (t *Tracker) Returned(token string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.detached || token == "" {
		return false
	}
	e, ok := t.byToken[token]
	if !ok {
		return false
	}
	e.returned = true
	return true
}

// ReturnedOnly records a `basic.return` for a tracker that has exactly one
// outstanding publication, which is [PerMessage]'s case and the reason that
// kind needs no correlation token.
//
// It reports false when the tracker holds any number of publications other
// than one, because then the return is not attributable and a driver that
// attributed it anyway would tell one publication another's outcome. A driver
// using a windowed or asynchronous strategy must use [Tracker.Returned].
func (t *Tracker) ReturnedOnly() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.detached || len(t.live) != 1 {
		return false
	}
	for _, e := range t.live {
		e.returned = true
	}
	return true
}

// Detach records that the channel closed and resolves every outstanding
// publication as [Ambiguous].
//
// It is the only place in this package that produces an ambiguous outcome, and
// it is terminal: a later confirm is discarded, and a later [Tracker.Submit]
// is refused with [ErrDetached]. Both halves matter. The ambiguity is the specification's
// rule — the broker may have taken every one of these publications, so a
// definite failure would invite a retry that duplicates them, which the specification
// names a settlement disqualifier — and the terminality is the staleness guard
// described on [Tracker].
//
// Calling it twice resolves nothing the second time.
func (t *Tracker) Detach() []Outcome {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.detached {
		return nil
	}
	t.detached = true
	out := make([]Outcome, 0, len(t.live))
	// The outcomes come out in tag order, which is the order the caller
	// published in. Nothing in the protocol requires it and a caller
	// reconciling a batch against its own slice does, so paying for the sort
	// here is cheaper than every caller paying for it.
	for n := t.lowest; n < t.next; n++ {
		e, ok := t.live[n]
		if !ok {
			continue
		}
		delete(t.live, n)
		if e.token != "" {
			delete(t.byToken, e.token)
		}
		t.resolved++
		t.ambiguous++
		out = append(out, Outcome{Tag: e.tag, Token: e.token, Result: Ambiguous, Returned: e.returned})
	}
	t.lowest = t.next
	return out
}

// Outstanding is how many publications are awaiting a confirm.
func (t *Tracker) Outstanding() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.live)
}

// Detached reports whether the channel this tracker belongs to has closed.
func (t *Tracker) Detached() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.detached
}

// Next is the tag the next publication would be given, which is one more than
// the last one issued.
func (t *Tracker) Next() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.next
}

// Stats is what the tracker has done: publications resolved and, of those, how
// many were resolved ambiguously by a detach.
//
// The second number is the specification's "ambiguity accuracy" as an observation. It is
// exported because a driver reports it through `mq.Hooks` and because the family's model asserts on it: a kind whose documented
// [Strategy.MaxAmbiguous] is one may never produce two.
func (t *Tracker) Stats() (resolved, ambiguous uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.resolved, t.ambiguous
}

// The tracker's refusals.
var (
	// ErrDetached is a publication submitted to a tracker whose channel has
	// closed. It carries CodeClosed because that is what it is to the
	// caller: the channel is gone, and the driver's own reconnection is what
	// produces a new one.
	ErrDetached = errcode.New(errcode.Closed,
		"iqmq/rabbitmq: the confirm channel has closed; every publication it was holding was reported ambiguous")
	// errNotLastSubmitted is Unsubmit being given anything but the tag the
	// tracker issued last, which is the only publication that can still be
	// taken back.
	errNotLastSubmitted = errcode.New(errcode.InvalidArgument,
		"iqmq/rabbitmq: only the most recently submitted publication can be taken back; an earlier one has already been written")
	// ErrDuplicateToken is two outstanding publications under one
	// correlation token, which would make a `basic.return` attributable to
	// either.
	ErrDuplicateToken = errcode.New(errcode.InvalidArgument,
		"iqmq/rabbitmq: two outstanding publications share one confirm correlation token, so a basic.return could not be attributed to either")
)

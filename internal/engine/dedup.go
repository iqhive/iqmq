package engine

import (
	"sync"
	"time"

	"github.com/iqhive/iqmq/internal/errcode"
)

// dedupPruneFloor is the number of live keys a window holds before a
// publication pays for a sweep of the expired ones. Below it the window is
// small enough that the map itself is the cheaper bound.
const dedupPruneFloor = 64

// errDedupPending refuses a publication whose key belongs to a publication
// the engine is still admitting. Its outcome is not known yet: reporting the
// second one as a duplicate would be a success for a message nothing may
// admit, if the first is refused. It is retryable, and the retry finds the
// key either taken or free.
var errDedupPending = errcode.New(errcode.Unavailable,
	"iqmq/engine: a publication with this deduplication key is still being admitted")

// dedupWindow is the publication-key window of the message contract: a
// publication carrying a key the topic has already seen inside the window is
// discarded rather than admitted.
//
// It is a map from key to expiry, swept lazily: an expired entry is treated
// as absent when it is looked up, and the whole map is swept once it has
// grown to twice what the last sweep left (and at least dedupPruneFloor), so
// a topic that publishes a fresh key every time pays for each sweep with as
// many publications as the sweep visits and never holds more than twice the
// keys the window admits.
type dedupWindow struct {
	window  int64
	mu      sync.Mutex
	seen    map[string]dedupEntry
	sweepAt int
	// sweeps counts the sweeps, which is what holds the amortisation above.
	sweeps int
}

// dedupEntry is one key of the window. A pending key belongs to a
// publication the engine has not yet admitted or refused (dedupWindow.commit,
// dedupWindow.forget); it never expires and is never swept, because its
// publication still owns it.
type dedupEntry struct {
	at      int64
	pending bool
}

// newDedupWindow returns a window of d.
func newDedupWindow(d time.Duration) *dedupWindow {
	return &dedupWindow{window: int64(d), seen: make(map[string]dedupEntry), sweepAt: dedupPruneFloor}
}

// admit decides a publication carrying key at now: nil admits it and
// registers the key as pending, errDedupPending refuses it while another
// publication of the key is being admitted, and dup reports a duplicate of an
// admitted publication inside the window. A duplicate does not extend the
// window: the key expires a window after the publication that was admitted,
// which is what makes the window a bound on how long a duplicate is
// remembered rather than a bound a duplicate can push forward for ever.
//
// The window is not started here. A pending key never expires, and the
// publication that owns it may wait for room under OverflowBlock for longer
// than the window; an expiry fixed now had passed by the time that
// publication was admitted, so a duplicate published right after it was
// admitted too (commit).
func (d *dedupWindow) admit(key []byte, now int64) (dup bool, err error) {
	if len(key) == 0 {
		return false, nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if e, ok := d.seen[string(key)]; ok {
		if e.pending {
			return false, errDedupPending
		}
		if e.at > now {
			return true, nil
		}
	}
	if len(d.seen) >= d.sweepAt {
		d.sweepLocked(now)
		d.sweepAt = max(dedupPruneFloor, 2*len(d.seen))
	}
	d.seen[string(key)] = dedupEntry{pending: true}
	return false, nil
}

// commit marks a pending key as taken by the publication the engine accepted
// at now, which is when a later publication of it becomes a duplicate, and
// starts the key's window there. A publication the engine accepted keeps its
// key, whether a subscription held it, the retained log took it or an
// overflow policy discarded it.
func (d *dedupWindow) commit(key []byte, now int64) {
	if len(key) == 0 {
		return
	}
	d.mu.Lock()
	if _, ok := d.seen[string(key)]; ok {
		d.seen[string(key)] = dedupEntry{at: later(now, time.Duration(d.window))}
	}
	d.mu.Unlock()
}

// forget gives a pending key back to the window, which the publish path does
// for a publication the engine refused.
//
// The window has to be written before the capacity is reserved — two
// publications of one key must not both pass the check — so a refusal has
// already registered the key by the time it happens. Leaving it there would
// make the retry the error asks for a duplicate: reported as success, its
// payload released and nothing admitted, which is a lost message.
func (d *dedupWindow) forget(key []byte) {
	if len(key) == 0 {
		return
	}
	d.mu.Lock()
	delete(d.seen, string(key))
	d.mu.Unlock()
}

// sweepLocked removes every key whose window has closed.
func (d *dedupWindow) sweepLocked(now int64) {
	d.sweeps++
	for k, e := range d.seen {
		if !e.pending && e.at <= now {
			delete(d.seen, k)
		}
	}
}

// len returns the number of keys the window remembers, expired ones included.
func (d *dedupWindow) len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.seen)
}

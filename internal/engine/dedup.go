package engine

import (
	"sync"
	"time"
)

// dedupPruneFloor is the number of live keys a window holds before a
// publication pays for a sweep of the expired ones. Below it the window is
// small enough that the map itself is the cheaper bound.
const dedupPruneFloor = 64

// dedupWindow is the publication-key window of the message contract: a
// publication carrying a key the topic has already seen inside the window is
// discarded rather than admitted.
//
// It is a map from key to expiry, swept lazily: an expired entry is treated
// as absent when it is looked up, and the whole map is swept when it has
// grown past dedupPruneFloor, so a topic that publishes a fresh key every
// time pays one sweep per dedupPruneFloor publications and never holds more
// keys than the window admits.
type dedupWindow struct {
	window int64
	mu     sync.Mutex
	seen   map[string]int64
}

// newDedupWindow returns a window of d.
func newDedupWindow(d time.Duration) *dedupWindow {
	return &dedupWindow{window: int64(d), seen: make(map[string]int64)}
}

// admit reports whether a publication carrying key is admitted at now. A
// second publication of the same key inside the window is refused, and the
// window is not extended by it: the key expires a window after the
// publication that was admitted, which is what makes the window a bound on
// how long a duplicate is remembered rather than a bound a duplicate can
// push forward for ever.
func (d *dedupWindow) admit(key []byte, now int64) bool {
	if len(key) == 0 {
		return true
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if at, ok := d.seen[string(key)]; ok && at > now {
		return false
	}
	if len(d.seen) >= dedupPruneFloor {
		d.sweepLocked(now)
	}
	d.seen[string(key)] = now + d.window
	return true
}

// forget gives a key back to the window, which the publish path does for a
// publication the engine refused.
//
// The window has to be written before the capacity is reserved — two
// publications of one key must not both pass the check — so a refusal has
// already registered the key by the time it happens. Leaving it there would
// make the retry the error asks for a duplicate: reported as success, its
// payload released and nothing admitted, which is a lost message. A
// publication the engine *accepted* keeps its key, whether a subscription
// held it, the retained log took it or an overflow policy discarded it.
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
	for k, at := range d.seen {
		if at <= now {
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

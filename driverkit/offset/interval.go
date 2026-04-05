package offset

// span is a half-open range of resolved offsets, [lo, hi).
type span struct{ lo, hi int64 }

// interval is the range-set tracker: instead of a bit or a map entry per
// offset it keeps the merged ranges of *resolved* offsets above the
// watermark. An offset is resolved when it completed, or when the broker
// never delivered it (a compacted record or a transaction marker, recorded by
// Track when the offset jumps). The watermark advances by swallowing the
// leading range.
//
// Memory is proportional to the number of holes rather than to the window, so
// a wide window with few in-flight records costs little; the cost is the
// ordered insert, which shifts the ranges above the completed offset.
type interval struct {
	common
	// res holds the merged, sorted resolved ranges within [base, high).
	// res[head:] is live; head lets the leading range be dropped without
	// shifting the rest.
	res  []span
	head int
}

func newInterval(cfg Config) *interval {
	return &interval{common: newCommon(Interval, cfg), res: make([]span, 0, 64)}
}

// Track implements Tracker.
func (t *interval) Track(gen uint32, off int64) error {
	if err := t.checkTrack(gen, off); err != nil {
		return err
	}
	switch {
	case t.pend == 0:
		// Nothing is in flight: the watermark jumps to the new record and no
		// range needs recording at all.
		t.base = off
	case off > t.high:
		// The broker skipped [high, off): those offsets are resolved and must
		// not block the watermark. They always sit above every recorded
		// range, so appending (with a merge onto the last one) suffices.
		t.appendSpan(t.high, off)
	}
	t.high = off + 1
	t.pend++
	return nil
}

// appendSpan adds a range above every recorded one.
func (t *interval) appendSpan(lo, hi int64) {
	if n := len(t.res); n > t.head && t.res[n-1].hi == lo {
		t.res[n-1].hi = hi
		return
	}
	t.res = append(t.res, span{lo, hi})
}

// Complete implements Tracker.
func (t *interval) Complete(gen uint32, off int64) (int64, bool) {
	if gen != t.gen || off < t.base || off >= t.high || t.resolved(off) {
		return t.base, false
	}
	t.insert(off)
	t.pend--
	before := t.base
	// Only the leading range can start at the watermark: the ranges are
	// merged and sorted, so at most one swallow is possible per completion.
	if live := t.res[t.head:]; len(live) > 0 && live[0].lo <= t.base {
		if live[0].hi > t.base {
			t.base = live[0].hi
		}
		t.res[t.head] = span{}
		t.head++
		t.compact()
	}
	return t.base, t.base > before
}

// search returns the index of the first range whose end is at or above off.
// It is written out rather than calling sort.Search so that the hot path
// takes no closure.
func search(live []span, off int64) int {
	lo, hi := 0, len(live)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if live[mid].hi < off {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// resolved reports whether off lies in a recorded range.
func (t *interval) resolved(off int64) bool {
	live := t.res[t.head:]
	i := search(live, off+1)
	return i < len(live) && live[i].lo <= off
}

// insert adds the single offset off to the range set, merging with the
// neighbours it touches.
func (t *interval) insert(off int64) {
	live := t.res[t.head:]
	i := search(live, off)
	lo, hi := off, off+1
	switch {
	case i < len(live) && live[i].hi == off:
		// Extends the range below; it may now touch the one above.
		lo = live[i].lo
		if i+1 < len(live) && live[i+1].lo == hi {
			hi = live[i+1].hi
			t.remove(i + 1)
			live = t.res[t.head:]
		}
		live[i] = span{lo, hi}
	case i < len(live) && live[i].lo == hi:
		// Extends the range above.
		live[i] = span{lo, live[i].hi}
	default:
		t.insertAt(i, span{lo, hi})
	}
}

// insertAt inserts s at position i of the live ranges.
func (t *interval) insertAt(i int, s span) {
	t.res = append(t.res, span{})
	live := t.res[t.head:]
	copy(live[i+1:], live[i:])
	live[i] = s
}

// remove deletes position i of the live ranges.
func (t *interval) remove(i int) {
	live := t.res[t.head:]
	copy(live[i:], live[i+1:])
	t.res = t.res[:len(t.res)-1]
}

// compact reclaims the dropped prefix once it dominates the slice.
func (t *interval) compact() {
	if t.head >= 32 && t.head*2 >= len(t.res) {
		n := copy(t.res, t.res[t.head:])
		t.res, t.head = t.res[:n], 0
	}
}

// Reset implements Tracker.
func (t *interval) Reset(next int64) uint32 {
	t.res, t.head = t.res[:0], 0
	return t.rebase(next)
}

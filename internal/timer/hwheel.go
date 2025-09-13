package timer

import (
	"math/bits"
	"time"
)

// Hierarchical wheel geometry: 4 levels of 256 slots, 1 ms ticks.
const (
	hwLevels   = 4
	hwSlotBits = 8
	hwSlots    = 1 << hwSlotBits
	hwSlotMask = hwSlots - 1
	hwWords    = hwSlots / 64
	hwSpan     = int64(1) << (hwSlotBits * hwLevels) // ticks the wheel covers
)

// hwheel is a cascading timing wheel. Level l slot s covers the ticks whose
// bits [8l, 8l+8) equal s; an entry is filed at the lowest level whose lap
// contains its distance from the cursor, and cascades down when the cursor
// reaches the start of its slot. Entries at least hwSpan ticks ahead wait
// in an overflow heap and migrate into the wheel as the cursor advances.
//
// cur is the tick of the last now passed to Fire (or New); every entry with
// an earlier tick has fired. Invariant: at every level above 0 the slot the
// cursor is in holds only entries of the next lap, because it was cascaded
// when the cursor entered it; level 0's cursor slot holds only entries due
// at cur. Next and Fire therefore know exactly which lap every occupied
// slot belongs to.
type hwheel[V any] struct {
	cur      int64
	n        int
	slots    [hwLevels][hwSlots]bucket[V]
	occupied [hwLevels][hwWords]uint64
	overflow heap[V]
}

func newHWheel[V any](now int64) *hwheel[V] {
	return &hwheel[V]{cur: floorTick(now), overflow: heap[V]{arity: 4}}
}

func (w *hwheel[V]) Resolution() time.Duration { return tick }

func (w *hwheel[V]) Len() int { return w.n }

// tickOf returns the tick e fires at, never before the cursor.
func (w *hwheel[V]) tickOf(e *Entry[V]) int64 { return max(ceilTick(e.due), w.cur) }

// place files an armed entry by its distance from the cursor.
func (w *hwheel[V]) place(e *Entry[V]) {
	t := w.tickOf(e)
	delta := t - w.cur
	if delta >= hwSpan {
		w.overflow.push(e)
		return
	}
	l := 0
	for delta >= int64(1)<<(hwSlotBits*(l+1)) {
		l++
	}
	slot := int((t >> (hwSlotBits * l)) & hwSlotMask)
	e.where = whereWheel
	e.idx = int32(l<<hwSlotBits | slot)
	w.slots[l][slot].push(e)
	w.occupied[l][slot>>6] |= 1 << (slot & 63)
}

func (w *hwheel[V]) Add(e *Entry[V], due int64) {
	e.checkFree()
	e.arm(due, whereWheel)
	w.place(e)
	w.n++
}

func (w *hwheel[V]) Cancel(e *Entry[V]) bool {
	switch e.where {
	case whereWheel:
		l, slot := int(e.idx>>hwSlotBits), int(e.idx&hwSlotMask)
		b := &w.slots[l][slot]
		b.remove(e)
		if b.n == 0 {
			w.occupied[l][slot>>6] &^= 1 << (slot & 63)
		}
	case whereHeap:
		w.overflow.removeAt(int(e.idx))
	default:
		return false
	}
	e.disarm()
	w.n--
	return true
}

// nextOccupied returns the first occupied slot of level l at or after from,
// or -1.
func (w *hwheel[V]) nextOccupied(l, from int) int {
	if from >= hwSlots {
		return -1
	}
	word := from >> 6
	if m := w.occupied[l][word] >> (from & 63); m != 0 {
		return from + bits.TrailingZeros64(m)
	}
	for word++; word < hwWords; word++ {
		if m := w.occupied[l][word]; m != 0 {
			return word<<6 + bits.TrailingZeros64(m)
		}
	}
	return -1
}

// nextEvent returns the earliest tick at which the wheel has work: the
// start of the first occupied slot of every level (in the current lap, or
// the next lap for slots at or behind the cursor) and the overflow minimum.
// It is never later than the first firing and equals it once the entry has
// cascaded to level 0. The second result is false when the wheel is empty;
// ticks are negative on a negative origin, so emptiness cannot be signalled
// by the value.
func (w *hwheel[V]) nextEvent() (int64, bool) {
	best, ok := int64(0), false
	for l := range hwLevels {
		shift := uint(hwSlotBits * l)
		from := int((w.cur >> shift) & hwSlotMask)
		lap := (w.cur >> (shift + hwSlotBits)) << (shift + hwSlotBits)
		start := from + 1
		if l == 0 {
			start = from // level 0's cursor slot holds entries due now
		}
		var t int64
		if s := w.nextOccupied(l, start); s >= 0 {
			t = lap | int64(s)<<shift
		} else if s := w.nextOccupied(l, 0); s >= 0 {
			t = lap + int64(1)<<(shift+hwSlotBits) + int64(s)<<shift
		} else {
			continue
		}
		if !ok || t < best {
			best, ok = t, true
		}
	}
	if e := w.overflow.top(); e != nil {
		if t := w.tickOf(e); !ok || t < best {
			best, ok = t, true
		}
	}
	return best, ok
}

func (w *hwheel[V]) Next() (int64, bool) {
	nxt, ok := w.nextEvent()
	if !ok {
		return 0, false
	}
	return tickTime(nxt), true
}

// setCur moves the cursor to c and cascades every slot that starts there.
func (w *hwheel[V]) setCur(c int64) {
	w.cur = c
	if c&hwSlotMask != 0 {
		return
	}
	for l := 1; l < hwLevels; l++ {
		s := int((c >> (hwSlotBits * l)) & hwSlotMask)
		w.cascade(l, s)
		if s != 0 {
			return
		}
	}
}

// cascade re-files every entry of level l slot s, which the cursor has
// just reached, into lower levels.
func (w *hwheel[V]) cascade(l, s int) {
	b := &w.slots[l][s]
	if b.n == 0 {
		return
	}
	e := b.takeAll()
	w.occupied[l][s>>6] &^= 1 << (s & 63)
	for e != nil {
		next := e.next
		w.place(e)
		e = next
	}
}

// migrate moves overflow entries that now fit in the wheel.
func (w *hwheel[V]) migrate() {
	for e := w.overflow.top(); e != nil && w.tickOf(e)-w.cur < hwSpan; e = w.overflow.top() {
		w.overflow.pop()
		w.place(e)
	}
}

// fireSlot empties level 0 slot s into dst.
func (w *hwheel[V]) fireSlot(s int, dst []*Entry[V]) []*Entry[V] {
	e := w.slots[0][s].takeAll()
	w.occupied[0][s>>6] &^= 1 << (s & 63)
	for e != nil {
		next := e.next
		e.disarm()
		w.n--
		dst = append(dst, e)
		e = next
	}
	return dst
}

func (w *hwheel[V]) Fire(now int64, dst []*Entry[V]) []*Entry[V] {
	target := max(floorTick(now), w.cur)
	for {
		nxt, ok := w.nextEvent()
		if !ok || nxt > target {
			// Nothing starts in (cur, target]: no slot needs cascading on
			// the way, so the cursor may jump.
			w.cur = target
			return dst
		}
		if nxt > w.cur {
			w.setCur(nxt)
		}
		// Overflow entries now within the span enter the wheel; one due at
		// the cursor lands in the cursor slot and fires below.
		w.migrate()
		if s := int(w.cur & hwSlotMask); w.occupied[0][s>>6]&(1<<(s&63)) != 0 {
			dst = w.fireSlot(s, dst)
		}
		if w.cur == target {
			return dst
		}
	}
}

package timer

import (
	"math/bits"
	"time"
)

// Hashed wheel geometry: 4096 slots of 1 ms (a 4.096 s lap).
const (
	hashSlotBits = 12
	hashSlots    = 1 << hashSlotBits
	hashSlotMask = hashSlots - 1
	hashWords    = hashSlots / 64
)

// hashedWheel files each entry in slot tick%4096 and keeps its absolute
// tick, so one slot can hold entries from several laps; visiting a slot
// fires only the entries whose tick has arrived. Add and Cancel are O(1);
// Fire visits every occupied slot at most once per lap; Next is the cached
// minimum tick, recomputed from per-slot minima when unknown.
//
// cur is the tick of the last now passed to Fire (or New).
//
// The slots are made by the first Add rather than by New: they are about
// 132 KiB, and the engine builds a wheel for every lane store (and takes a
// Fresh one at every time-to-live sweep) whether or not the store ever
// delays a record. Once made they are kept, so a wheel that empties and
// fills again allocates nothing.
type hashedWheel[V any] struct {
	cur     int64
	n       int
	minTick int64 // earliest tick overall, tickNone when unknown
	id      owner
	s       *hashedSlots[V] // nil until the first Add
}

// hashedSlots is the wheel's storage. Every method that reads it runs only
// once an entry has been added, which is what makes it safe to make late.
type hashedSlots[V any] struct {
	slots    [hashSlots]bucket[V]
	occupied [hashWords]uint64
	slotMin  [hashSlots]int64 // earliest tick in the slot, tickNone when unknown
}

func newHashedWheel[V any](now int64) *hashedWheel[V] {
	return &hashedWheel[V]{cur: floorTick(now), minTick: tickNone}
}

func (w *hashedWheel[V]) Resolution() time.Duration { return tick }

func (w *hashedWheel[V]) Len() int { return w.n }

func (w *hashedWheel[V]) Fresh(now int64) Scheduler[V] { return newHashedWheel[V](now) }

func (w *hashedWheel[V]) Add(e *Entry[V], due int64) {
	if e.foreign(&w.id) {
		return
	}
	if e.Scheduled() {
		w.Cancel(e)
	}
	e.arm(due, whereWheel, &w.id)
	if w.s == nil {
		w.s = new(hashedSlots[V])
	}
	s := w.s
	t := ceilTick(due)
	slot := int(max(t, w.cur) & hashSlotMask)
	e.idx = int32(slot)
	b := &s.slots[slot]
	switch {
	case b.n == 0:
		s.slotMin[slot] = t
		s.occupied[slot>>6] |= 1 << (slot & 63)
	case s.slotMin[slot] != tickNone && t < s.slotMin[slot]:
		s.slotMin[slot] = t
	}
	b.push(e)
	switch {
	case w.n == 0:
		w.minTick = t
	case w.minTick != tickNone && t < w.minTick:
		w.minTick = t
	}
	w.n++
}

func (w *hashedWheel[V]) Cancel(e *Entry[V]) bool {
	if e.where != whereWheel || e.owner != &w.id {
		return false
	}
	s := w.s
	slot := int(e.idx)
	b := &s.slots[slot]
	b.remove(e)
	t := ceilTick(e.due)
	if b.n == 0 {
		s.occupied[slot>>6] &^= 1 << (slot & 63)
	} else if s.slotMin[slot] == t {
		s.slotMin[slot] = tickNone
	}
	if w.minTick == t {
		w.minTick = tickNone
	}
	e.disarm()
	w.n--
	return true
}

// slotMinimum returns the earliest tick in occupied slot s, rescanning it
// when the cached value is unknown.
func (w *hashedWheel[V]) slotMinimum(s int) int64 {
	ws := w.s
	if ws.slotMin[s] == tickNone {
		m := int64(tickNone)
		for e := ws.slots[s].head; e != nil; e = e.next {
			if t := ceilTick(e.due); m == tickNone || t < m {
				m = t
			}
		}
		ws.slotMin[s] = m
	}
	return ws.slotMin[s]
}

// scanMin recomputes minTick from the per-slot minima.
func (w *hashedWheel[V]) scanMin() {
	w.minTick = tickNone
	for word, m := range &w.s.occupied {
		for m != 0 {
			s := word<<6 + bits.TrailingZeros64(m)
			m &= m - 1
			if t := w.slotMinimum(s); w.minTick == tickNone || t < w.minTick {
				w.minTick = t
			}
		}
	}
}

func (w *hashedWheel[V]) Next() (int64, bool) {
	if w.n == 0 {
		return 0, false
	}
	if w.minTick == tickNone {
		w.scanMin()
	}
	return tickTime(w.minTick), true
}

// nextOccupied returns the first occupied slot at or after from, or -1.
func (w *hashedWheel[V]) nextOccupied(from int) int {
	if from >= hashSlots {
		return -1
	}
	occupied := &w.s.occupied
	word := from >> 6
	if m := occupied[word] >> (from & 63); m != 0 {
		return from + bits.TrailingZeros64(m)
	}
	for word++; word < hashWords; word++ {
		if m := occupied[word]; m != 0 {
			return word<<6 + bits.TrailingZeros64(m)
		}
	}
	return -1
}

// visit fires the entries of slot s whose tick is at or before target and
// refreshes the slot's minimum.
func (w *hashedWheel[V]) visit(s int, target int64, dst []*Entry[V]) []*Entry[V] {
	ws := w.s
	if m := ws.slotMin[s]; m != tickNone && m > target {
		return dst
	}
	b := &ws.slots[s]
	rest := int64(tickNone)
	for e := b.head; e != nil; {
		next := e.next
		if t := ceilTick(e.due); t <= target {
			b.remove(e)
			e.disarm()
			w.n--
			dst = append(dst, e)
		} else if rest == tickNone || t < rest {
			rest = t
		}
		e = next
	}
	if b.n == 0 {
		ws.occupied[s>>6] &^= 1 << (s & 63)
	}
	ws.slotMin[s] = rest
	return dst
}

func (w *hashedWheel[V]) Fire(now int64, dst []*Entry[V]) []*Entry[V] {
	target := max(fireTick(now), w.cur)
	if w.n == 0 || (w.minTick != tickNone && w.minTick > target) {
		w.cur = target
		return dst
	}
	// Visit the occupied slots in tick order from the cursor, at most one
	// full lap: every entry carries its own tick, so one visit per slot
	// suffices however far target is.
	end := min(target, w.cur+hashSlotMask)
	for t := w.cur; t <= end; {
		s := w.nextOccupied(int(t & hashSlotMask))
		if s < 0 {
			t = (t | hashSlotMask) + 1
			continue
		}
		t = (t &^ hashSlotMask) | int64(s)
		if t > end {
			break
		}
		dst = w.visit(s, target, dst)
		t++
	}
	w.cur = target
	if w.minTick != tickNone && w.minTick <= target {
		w.minTick = tickNone // fired; recomputed lazily by Next
	}
	return dst
}

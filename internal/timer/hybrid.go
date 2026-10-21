package timer

import (
	"math/bits"
	"time"
)

// Hybrid geometry: a 1024-slot 1 ms wheel (about one second) fed by a heap.
const (
	hyBits    = 10
	hySlots   = 1 << hyBits
	hyMask    = hySlots - 1
	hyWords   = hySlots / 64
	hyHorizon = hySlots - 1 // ticks ahead of cur that fit in the wheel
)

// hybrid keeps near entries in a single-level wheel and far entries in a
// 4-ary heap. Fire first migrates heap entries that entered the horizon,
// then advances the wheel. Near-term operations are O(1); far-term ones
// O(log n) on a heap that only holds the tail of the distribution.
//
// cur is the tick of the last now passed to Fire (or New). Wheel entries
// have ticks in [cur, cur+hyHorizon]: slots at or after the cursor belong
// to the current lap, slots behind it to the next.
type hybrid[V any] struct {
	cur      int64
	nWheel   int
	slots    [hySlots]bucket[V]
	occupied [hyWords]uint64
	far      heap[V]
	id       owner
}

func newHybrid[V any](now int64) *hybrid[V] {
	return &hybrid[V]{cur: floorTick(now), far: heap[V]{arity: 4}}
}

func (w *hybrid[V]) Resolution() time.Duration { return tick }

func (w *hybrid[V]) Len() int { return w.nWheel + len(w.far.items) }

func (w *hybrid[V]) Fresh(now int64) Scheduler[V] { return newHybrid[V](now) }

// placeWheel files e at tick t, which must be within the horizon.
func (w *hybrid[V]) placeWheel(e *Entry[V], t int64) {
	slot := int(t & hyMask)
	e.idx = int32(slot)
	e.where = whereWheel
	w.slots[slot].push(e)
	w.occupied[slot>>6] |= 1 << (slot & 63)
	w.nWheel++
}

func (w *hybrid[V]) Add(e *Entry[V], due int64) {
	if e.foreign(&w.id) {
		return
	}
	if e.Scheduled() {
		w.Cancel(e)
	}
	if t := max(ceilTick(due), w.cur); t-w.cur <= hyHorizon {
		e.arm(due, whereWheel, &w.id)
		w.placeWheel(e, t)
		return
	}
	e.arm(due, whereHeap, &w.id)
	w.far.push(e)
}

func (w *hybrid[V]) Cancel(e *Entry[V]) bool {
	if e.owner != &w.id {
		return false
	}
	switch e.where {
	case whereWheel:
		slot := int(e.idx)
		b := &w.slots[slot]
		b.remove(e)
		if b.n == 0 {
			w.occupied[slot>>6] &^= 1 << (slot & 63)
		}
		w.nWheel--
		e.disarm()
		return true
	case whereHeap:
		w.far.removeAt(int(e.idx))
		e.disarm()
		return true
	default:
		return false
	}
}

func (w *hybrid[V]) nextOccupied(from int) int {
	if from >= hySlots {
		return -1
	}
	word := from >> 6
	if m := w.occupied[word] >> (from & 63); m != 0 {
		return from + bits.TrailingZeros64(m)
	}
	for word++; word < hyWords; word++ {
		if m := w.occupied[word]; m != 0 {
			return word<<6 + bits.TrailingZeros64(m)
		}
	}
	return -1
}

// wheelNext returns the earliest occupied tick in the wheel; the second
// result is false when the wheel holds nothing. Ticks are negative on a
// negative origin, so emptiness cannot be signalled by the value.
func (w *hybrid[V]) wheelNext() (int64, bool) {
	lap := w.cur &^ hyMask
	if s := w.nextOccupied(int(w.cur & hyMask)); s >= 0 {
		return lap | int64(s), true
	}
	if s := w.nextOccupied(0); s >= 0 {
		return lap + hySlots + int64(s), true
	}
	return 0, false
}

func (w *hybrid[V]) Next() (int64, bool) {
	best, ok := w.wheelNext()
	if e := w.far.top(); e != nil {
		if t := max(ceilTick(e.due), w.cur); !ok || t < best {
			best, ok = t, true
		}
	}
	if !ok {
		return 0, false
	}
	return tickTime(best), true
}

// migrate moves heap entries that now fit in the wheel.
func (w *hybrid[V]) migrate() {
	for e := w.far.top(); e != nil; e = w.far.top() {
		t := max(ceilTick(e.due), w.cur)
		if t-w.cur > hyHorizon {
			return
		}
		w.far.pop()
		w.placeWheel(e, t)
	}
}

// fireSlot empties slot s into dst.
func (w *hybrid[V]) fireSlot(s int, dst []*Entry[V]) []*Entry[V] {
	e := w.slots[s].takeAll()
	w.occupied[s>>6] &^= 1 << (s & 63)
	for e != nil {
		next := e.next
		e.disarm()
		w.nWheel--
		dst = append(dst, e)
		e = next
	}
	return dst
}

func (w *hybrid[V]) Fire(now int64, dst []*Entry[V]) []*Entry[V] {
	target := max(fireTick(now), w.cur)
	for {
		w.migrate()
		if w.nWheel == 0 {
			e := w.far.top()
			if e == nil {
				w.cur = target
				return dst
			}
			t := max(ceilTick(e.due), w.cur) // beyond the horizon, or migrate took it
			if t > target {
				w.cur = target
				return dst
			}
			w.cur = t - hyHorizon // bring the heap top into the wheel
			continue
		}
		s := w.nextOccupied(int(w.cur & hyMask))
		if s < 0 {
			// The rest of this lap is empty; the occupied slots are next lap.
			lapEnd := (w.cur | hyMask) + 1
			if lapEnd > target {
				w.cur = target
				return dst
			}
			w.cur = lapEnd
			continue
		}
		t := (w.cur &^ hyMask) | int64(s)
		if t > target {
			w.cur = target
			return dst
		}
		w.cur = t
		dst = w.fireSlot(s, dst)
		if t == target {
			return dst
		}
	}
}

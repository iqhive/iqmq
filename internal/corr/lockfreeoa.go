package corr

import "sync/atomic"

// Slot states. The state word is the slot's ownership token: a writer moves
// it empty → writing → ready, a taker ready → taking → empty. Only the owner
// of a writing or taking slot touches its value, and the release/acquire pair
// on the state word publishes what it wrote.
const (
	slotEmpty uint32 = iota
	slotWriting
	slotReady
	slotTaking
)

// probeLimit bounds the probe sequence of one id: an entry always lives
// within probeLimit slots of its home, so a lookup that walks them all and
// finds nothing is a definite miss — no chain-termination marker is needed and
// a freed slot costs nothing but its key, which stays as a tombstone so that
// chains through it survive.
//
// The table is four times the capacity, so a Put refuses for want of room
// only when probeLimit consecutive slots are occupied: with the table at most
// a quarter full that needs 4^-probeLimit luck, and for a capacity below
// probeLimit/4 the window covers the whole table and the refusal is exact.
// The price of the tombstones is that a lookup which misses walks the window
// once every slot in it has been used at least once; the sweep never
// reclaims them, which is one of the things the bake-off weighs.
const probeLimit = 32

// lockFreeOA is the open-addressing store: a fixed table of slots, each with
// its own state word, so Put, Take and Expire take no lock and never allocate.
// The table is sized to four times the capacity, so probes stay short.
//
// The trade the bake-off weighs: Take is a couple of atomic operations on one
// cache line, but Expire has to sweep the whole table rather than the entries.
type lockFreeOA[T any] struct {
	slots []oaSlot[T]
	mask  uint64
	n     counter
}

type oaSlot[T any] struct {
	state    atomic.Uint32
	key      atomic.Uint64
	deadline atomic.Int64
	value    T
}

func newLockFreeOA[T any](cfg Config) *lockFreeOA[T] {
	size := pow2ceil(4 * cfg.Capacity)
	s := &lockFreeOA[T]{
		slots: make([]oaSlot[T], size),
		mask:  uint64(size - 1),
	}
	s.n.cap = int64(cfg.Capacity)
	return s
}

// probes returns the number of slots to examine for one id.
func (s *lockFreeOA[T]) probes() int {
	if n := len(s.slots); n < probeLimit {
		return n
	}
	return probeLimit
}

// Put implements Store. The slot is reserved on the counter first, as in the
// sharded store, so the capacity bound holds and Len stays non-negative.
func (s *lockFreeOA[T]) Put(id ID, deadline int64, v T) bool {
	if id == 0 || !s.n.reserve() {
		return false
	}
	h := mix(id)
	limit := s.probes()
	// Rescan until a whole pass over the probe window finds no free slot.
	// Losing the slot the scan chose is not fullness — the loser's window
	// still has room, and the entry it was asked to place is one the caller
	// is entitled to — so it rescans rather than refusing. Every loss is
	// another writer's claim, so a turn of this loop is always preceded by
	// someone's progress, and the exits are a placement, a duplicate and a
	// window with nothing free. Giving up after two attempts instead made
	// the store refuse a Put while it held four entries of four thousand,
	// which is how the benchmark lost two of ten measured rounds.
	for {
		free := -1
		for i := range limit {
			idx := (h + uint64(i)) & s.mask
			slot := &s.slots[idx]
			st := slot.state.Load()
			key := slot.key.Load()
			if st == slotEmpty {
				if free < 0 {
					free = int(idx)
				}
				if key == 0 {
					// A slot never used stops the scan: an entry for this id
					// would have been placed at or before it.
					break
				}
				continue
			}
			if key == uint64(id) && (st == slotReady || st == slotWriting) {
				s.n.release(1)
				return false // already outstanding
			}
		}
		if free < 0 {
			s.n.release(1)
			return false // no room within the probe window
		}
		slot := &s.slots[free]
		if !slot.state.CompareAndSwap(slotEmpty, slotWriting) {
			continue // another writer took it; rescan
		}
		slot.key.Store(uint64(id))
		slot.deadline.Store(deadline)
		slot.value = v
		slot.state.Store(slotReady)
		return true
	}
}

// Take implements Store.
func (s *lockFreeOA[T]) Take(id ID) (T, bool) {
	var zero T
	if id == 0 {
		return zero, false
	}
	h := mix(id)
	for i := range s.probes() {
		slot := &s.slots[(h+uint64(i))&s.mask]
		st := slot.state.Load()
		key := slot.key.Load()
		if st == slotEmpty && key == 0 {
			break // never used: the chain ends here
		}
		if st != slotReady || key != uint64(id) {
			continue
		}
		if !slot.state.CompareAndSwap(slotReady, slotTaking) {
			continue // a concurrent Take or sweep won it
		}
		if slot.key.Load() != uint64(id) {
			// The slot was recycled between the load and the claim: hand it
			// back untouched.
			slot.state.Store(slotReady)
			continue
		}
		v := slot.value
		slot.value = zero
		slot.state.Store(slotEmpty)
		s.n.release(1)
		return v, true
	}
	return zero, false
}

// Expire implements Store.
func (s *lockFreeOA[T]) Expire(now int64, dst []Pending[T]) []Pending[T] {
	var zero T
	removed := 0
	for i := range s.slots {
		slot := &s.slots[i]
		if slot.state.Load() != slotReady || slot.deadline.Load() > now {
			continue
		}
		if !slot.state.CompareAndSwap(slotReady, slotTaking) {
			continue
		}
		// The slot is ours now: re-read what it holds, because it may have
		// been recycled since the deadline was read.
		deadline := slot.deadline.Load()
		if deadline > now {
			slot.state.Store(slotReady)
			continue
		}
		dst = append(dst, Pending[T]{ID: ID(slot.key.Load()), Deadline: deadline, Value: slot.value})
		slot.value = zero
		slot.state.Store(slotEmpty)
		removed++
	}
	if removed > 0 {
		s.n.release(int64(removed))
	}
	return dst
}

// Len implements Store.
func (s *lockFreeOA[T]) Len() int { return s.n.len() }

// Kind implements Store.
func (s *lockFreeOA[T]) Kind() Kind { return LockFreeOA }

package queue

import "sync/atomic"

// seqSlot is one Vyukov ring slot: seq == pos means writable for position
// pos, seq == pos+1 means readable for position pos.
type seqSlot[T any] struct {
	seq atomic.Uint64
	val T
}

// ring is the slot storage shared by SeqMPMC, MPSC and the sharded kinds.
// Slot addressing lives in slot.go / slot_unsafe.go (variant).
type ring[T any] struct {
	slots []seqSlot[T]
	mask  uint64
	sb    slotBase
}

// newRing allocates at least two slots: with a single slot the sequence
// values "written for pos" (pos+1) and "free for pos+cap" (pos+cap) coincide.
func newRing[T any](capacity int) ring[T] {
	n := pow2ceil(max(capacity, 2))
	r := ring[T]{slots: make([]seqSlot[T], n), mask: uint64(n - 1)}
	for i := range r.slots {
		r.slots[i].seq.Store(uint64(i))
	}
	r.initSlots()
	return r
}

func (r *ring[T]) capacity() int { return int(r.mask) + 1 }

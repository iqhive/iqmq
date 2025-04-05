//go:build iqmq_unsafe

package queue

import "unsafe"

// UnsafeSlots reports whether ring slots are addressed with unsafe pointer
// arithmetic (build tag iqmq_unsafe, variant) instead of bounds-checked
// slice indexing.
const UnsafeSlots = true

// slotBase caches the slice base and element size for pointer arithmetic.
type slotBase struct {
	base unsafe.Pointer
	size uintptr
}

func (r *ring[T]) initSlots() {
	r.sb.base = unsafe.Pointer(unsafe.SliceData(r.slots))
	r.sb.size = unsafe.Sizeof(r.slots[0])
}

// slot returns the slot for position pos without a bounds check.
func (r *ring[T]) slot(pos uint64) *seqSlot[T] {
	return (*seqSlot[T])(unsafe.Add(r.sb.base, uintptr(pos&r.mask)*r.sb.size))
}

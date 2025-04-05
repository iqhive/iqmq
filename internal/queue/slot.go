//go:build !iqmq_unsafe

package queue

// UnsafeSlots reports whether ring slots are addressed with unsafe pointer
// arithmetic (build tag iqmq_unsafe, variant) instead of bounds-checked
// slice indexing.
const UnsafeSlots = false

// slotBase carries nothing in the pure-Go build; the iqmq_unsafe build keeps
// the slice base and element size in it.
type slotBase struct{}

func (r *ring[T]) initSlots() { r.sb = slotBase{} }

// slot returns the slot for position pos.
func (r *ring[T]) slot(pos uint64) *seqSlot[T] { return &r.slots[pos&r.mask] }

//go:build !iqmq_debug

package pool

// Debug reports whether the iqmq_debug canaries are compiled in.
const Debug = false

// DoublePutPanic is the panic value raised by a second Put of an item that
// is already idle in the pool (iqmq_debug builds only).
const DoublePutPanic = "pool: double Put detected (iqmq_debug canary)"

// PoisonByte is the value recycled buffers are filled with on Put in
// iqmq_debug builds.
const PoisonByte = 0xA5

// canaryPruneFloor is the smallest canary table size at which iqmq_debug
// builds prune entries for collected items; release builds keep no table.
const canaryPruneFloor = 1024

// canary is empty in release builds; its methods compile to nothing.
type canary[T any] struct{}

func (canary[T]) put(*T) {}

func (canary[T]) get(*T) {}

// poison is a no-op in release builds.
func poison([]byte) {}

//go:build iqmq_debug

package pool

import (
	"sync"
	"weak"
)

// Debug reports whether the iqmq_debug canaries are compiled in.
const Debug = true

// DoublePutPanic is the panic value raised by a second Put of an item that
// is already idle in the pool (iqmq_debug builds only).
const DoublePutPanic = "pool: double Put detected (iqmq_debug canary)"

// PoisonByte is the value recycled buffers are filled with on Put in
// iqmq_debug builds.
const PoisonByte = 0xA5

// canary records every item Put and forgets it on Get; a Put of an item that
// is still recorded is a double Put. Keys are weak pointers so that items the
// pool has dropped (None, SyncPool after a GC cycle, bounded pools when full)
// are pruned once collected and an address reused by a new allocation never
// matches a stale entry.
type canary[T any] struct {
	mu      sync.Mutex
	idle    map[weak.Pointer[T]]struct{}
	pruneAt int
}

// canaryPruneFloor is the smallest canary table size at which entries for
// collected items are pruned.
const canaryPruneFloor = 1024

func (c *canary[T]) put(v *T) {
	w := weak.Make(v)
	c.mu.Lock()
	if c.idle == nil {
		c.idle = make(map[weak.Pointer[T]]struct{})
		c.pruneAt = canaryPruneFloor
	}
	if _, dup := c.idle[w]; dup {
		c.mu.Unlock()
		panic(DoublePutPanic)
	}
	c.idle[w] = struct{}{}
	if len(c.idle) > c.pruneAt {
		for k := range c.idle {
			if k.Value() == nil {
				delete(c.idle, k)
			}
		}
		c.pruneAt = max(canaryPruneFloor, 2*len(c.idle))
	}
	c.mu.Unlock()
}

func (c *canary[T]) get(v *T) {
	if v == nil {
		return
	}
	w := weak.Make(v)
	c.mu.Lock()
	delete(c.idle, w)
	c.mu.Unlock()
}

// poison overwrites a recycled buffer so a stale reader sees garbage.
func poison(b []byte) {
	for i := range b {
		b[i] = PoisonByte
	}
}

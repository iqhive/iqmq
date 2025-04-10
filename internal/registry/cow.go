package registry

import (
	"sync"
	"sync/atomic"
)

// cow is a copy-on-write registry: readers load an immutable map through an
// atomic pointer and never lock; writers serialise on a mutex, copy the
// map, apply the change and publish the copy. Writes cost O(n); reads and
// Range cost nothing beyond the map access itself.
type cow[V any] struct {
	cur atomic.Pointer[map[Key]V]
	mu  sync.Mutex // serialises writers
}

func newCOW[V any]() *cow[V] {
	c := &cow[V]{}
	m := make(map[Key]V)
	c.cur.Store(&m)
	return c
}

func (c *cow[V]) snapshot() map[Key]V { return *c.cur.Load() }

func (c *cow[V]) Load(k Key) (V, bool) {
	v, ok := c.snapshot()[k]
	return v, ok
}

func (c *cow[V]) LoadOrStore(k Key, v V) (V, bool) {
	if cur, ok := c.snapshot()[k]; ok {
		return cur, true
	}
	c.mu.Lock()
	old := c.snapshot()
	if cur, ok := old[k]; ok {
		c.mu.Unlock()
		return cur, true
	}
	next := make(map[Key]V, len(old)+1)
	for kk, vv := range old {
		next[kk] = vv
	}
	next[k] = v
	c.cur.Store(&next)
	c.mu.Unlock()
	return v, false
}

func (c *cow[V]) Delete(k Key) (V, bool) {
	var zero V
	if _, ok := c.snapshot()[k]; !ok {
		return zero, false
	}
	c.mu.Lock()
	old := c.snapshot()
	cur, ok := old[k]
	if !ok {
		c.mu.Unlock()
		return zero, false
	}
	next := make(map[Key]V, len(old)-1)
	for kk, vv := range old {
		if kk != k {
			next[kk] = vv
		}
	}
	c.cur.Store(&next)
	c.mu.Unlock()
	return cur, true
}

// Range visits the snapshot current when it was called.
func (c *cow[V]) Range(fn func(k Key, v V) bool) {
	for k, v := range c.snapshot() {
		if !fn(k, v) {
			return
		}
	}
}

func (c *cow[V]) Len() int { return len(c.snapshot()) }

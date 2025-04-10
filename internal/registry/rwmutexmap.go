package registry

import "sync"

// rwMutexMap is the reference registry: one RWMutex around a Go map.
type rwMutexMap[V any] struct {
	mu sync.RWMutex
	m  map[Key]V
}

func newRWMutexMap[V any]() *rwMutexMap[V] {
	return &rwMutexMap[V]{m: make(map[Key]V)}
}

func (r *rwMutexMap[V]) Load(k Key) (V, bool) {
	r.mu.RLock()
	v, ok := r.m[k]
	r.mu.RUnlock()
	return v, ok
}

func (r *rwMutexMap[V]) LoadOrStore(k Key, v V) (V, bool) {
	r.mu.RLock()
	if cur, ok := r.m[k]; ok {
		r.mu.RUnlock()
		return cur, true
	}
	r.mu.RUnlock()
	r.mu.Lock()
	if cur, ok := r.m[k]; ok {
		r.mu.Unlock()
		return cur, true
	}
	r.m[k] = v
	r.mu.Unlock()
	return v, false
}

func (r *rwMutexMap[V]) Delete(k Key) (V, bool) {
	r.mu.Lock()
	old, ok := r.m[k]
	if ok {
		delete(r.m, k)
	}
	r.mu.Unlock()
	return old, ok
}

// Range copies the entries under the read lock and visits the copy, so fn
// may call back into the registry without deadlocking.
func (r *rwMutexMap[V]) Range(fn func(k Key, v V) bool) {
	r.mu.RLock()
	snap := make([]entry[V], 0, len(r.m))
	for k, v := range r.m {
		snap = append(snap, entry[V]{k, v})
	}
	r.mu.RUnlock()
	for _, e := range snap {
		if !fn(e.k, e.v) {
			return
		}
	}
}

func (r *rwMutexMap[V]) Len() int {
	r.mu.RLock()
	n := len(r.m)
	r.mu.RUnlock()
	return n
}

// entry is one key/value pair of a Range snapshot.
type entry[V any] struct {
	k Key
	v V
}

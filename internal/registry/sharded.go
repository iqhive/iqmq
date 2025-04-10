package registry

import "sync"

// sharded splits the key space over power-of-two RWMutex-guarded maps so
// that writers to different topics and readers under a writer do not
// contend. Shards are padded to their own cache lines.
type sharded[V any] struct {
	shards []shard[V]
	mask   uint64
}

type shard[V any] struct {
	mu sync.RWMutex
	m  map[Key]V
	_  [cacheLine]byte // keeps the next shard's lock on another cache line
}

// newSharded builds a registry with at least n shards (rounded up to a
// power of two; n ≤ 0 means one shard).
func newSharded[V any](n int) *sharded[V] {
	n = pow2ceil(max(n, 1))
	s := &sharded[V]{shards: make([]shard[V], n), mask: uint64(n - 1)}
	for i := range s.shards {
		s.shards[i].m = make(map[Key]V)
	}
	return s
}

func (s *sharded[V]) shard(k Key) *shard[V] {
	return &s.shards[hashKey(k)&s.mask]
}

func (s *sharded[V]) Load(k Key) (V, bool) {
	sh := s.shard(k)
	sh.mu.RLock()
	v, ok := sh.m[k]
	sh.mu.RUnlock()
	return v, ok
}

func (s *sharded[V]) LoadOrStore(k Key, v V) (V, bool) {
	sh := s.shard(k)
	sh.mu.RLock()
	if cur, ok := sh.m[k]; ok {
		sh.mu.RUnlock()
		return cur, true
	}
	sh.mu.RUnlock()
	sh.mu.Lock()
	if cur, ok := sh.m[k]; ok {
		sh.mu.Unlock()
		return cur, true
	}
	sh.m[k] = v
	sh.mu.Unlock()
	return v, false
}

func (s *sharded[V]) Delete(k Key) (V, bool) {
	sh := s.shard(k)
	sh.mu.Lock()
	old, ok := sh.m[k]
	if ok {
		delete(sh.m, k)
	}
	sh.mu.Unlock()
	return old, ok
}

// Range visits each shard's entries from a copy taken under that shard's
// read lock; fn may call back into the registry.
func (s *sharded[V]) Range(fn func(k Key, v V) bool) {
	var snap []entry[V]
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		snap = snap[:0]
		for k, v := range sh.m {
			snap = append(snap, entry[V]{k, v})
		}
		sh.mu.RUnlock()
		for _, e := range snap {
			if !fn(e.k, e.v) {
				return
			}
		}
	}
}

func (s *sharded[V]) Len() int {
	n := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		n += len(sh.m)
		sh.mu.RUnlock()
	}
	return n
}

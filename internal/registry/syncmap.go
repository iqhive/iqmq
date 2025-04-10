package registry

import "sync"

// syncMap wraps sync.Map. Values are boxed into interfaces, which is free
// for pointer-shaped V (the engine stores *topic) and allocates otherwise.
//
// sync.Map has no length, and a side counter maintained by the stores and
// the deletes cannot supply one honestly: the map mutation and the counter
// update are two steps, so a store, a delete and a store of one key can
// apply both increments before the decrement and report more entries than
// the registry has ever held. Ordering the two steps needs mutual
// exclusion between the writers, which is the cost this kind exists to
// avoid. Len therefore counts the entries themselves — O(n), and never
// twice, because sync.Map.Range visits no key more than once
// (ADR-017).
type syncMap[V any] struct {
	m sync.Map
}

func newSyncMap[V any]() *syncMap[V] { return &syncMap[V]{} }

func (s *syncMap[V]) Load(k Key) (V, bool) {
	v, ok := s.m.Load(k)
	if !ok {
		var zero V
		return zero, false
	}
	return v.(V), true
}

func (s *syncMap[V]) LoadOrStore(k Key, v V) (V, bool) {
	actual, loaded := s.m.LoadOrStore(k, v)
	return actual.(V), loaded
}

func (s *syncMap[V]) Delete(k Key) (V, bool) {
	old, ok := s.m.LoadAndDelete(k)
	if !ok {
		var zero V
		return zero, false
	}
	return old.(V), true
}

// Range delegates to sync.Map.Range, which tolerates concurrent mutation
// and visits every entry present for the whole call exactly once.
func (s *syncMap[V]) Range(fn func(k Key, v V) bool) {
	s.m.Range(func(k, v any) bool { return fn(k.(Key), v.(V)) })
}

// Len walks the map. Every entry present for the whole call is counted
// exactly once and no absent key is counted, so the result lies between
// zero and the number of keys that existed during the call; it is not the
// count at any single instant (Kind.ExactLen reports false).
func (s *syncMap[V]) Len() int {
	n := 0
	s.m.Range(func(_, _ any) bool { n++; return true })
	return n
}

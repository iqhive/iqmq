package corr

import "sync"

// syncMap is the sync.Map store: the standard library's read-mostly map with
// an atomic length beside it, since sync.Map has none.
//
// LoadOrStore makes the duplicate check exact and LoadAndDelete makes Take and
// Expire at-most-once with no lock at all. The cost is one small allocation
// per request: the entry is stored behind a pointer so that the map's any
// value never boxes a struct.
type syncMap[T any] struct {
	m sync.Map // ID -> *entry[T]
	n counter
}

func newSyncMap[T any](cfg Config) *syncMap[T] {
	s := &syncMap[T]{}
	s.n.cap = int64(cfg.Capacity)
	return s
}

// Put implements Store. The slot is reserved on the counter first, as in the
// sharded store, so the capacity bound holds and Len stays non-negative.
func (s *syncMap[T]) Put(id ID, deadline int64, v T) bool {
	if id == 0 || !s.n.reserve() {
		return false
	}
	if _, loaded := s.m.LoadOrStore(id, &entry[T]{deadline: deadline, value: v}); loaded {
		s.n.release(1)
		return false
	}
	return true
}

// Take implements Store.
func (s *syncMap[T]) Take(id ID) (T, bool) {
	v, ok := s.m.LoadAndDelete(id)
	if !ok {
		var zero T
		return zero, false
	}
	s.n.release(1)
	return v.(*entry[T]).value, true
}

// Expire implements Store.
func (s *syncMap[T]) Expire(now int64, dst []Pending[T]) []Pending[T] {
	removed := 0
	s.m.Range(func(k, v any) bool {
		e := v.(*entry[T])
		if e.deadline > now {
			return true
		}
		// LoadAndDelete decides the race with a concurrent Take: only the
		// caller that removes the entry reports it.
		if got, ok := s.m.LoadAndDelete(k); ok {
			e = got.(*entry[T])
			dst = append(dst, Pending[T]{ID: k.(ID), Deadline: e.deadline, Value: e.value})
			removed++
		}
		return true
	})
	if removed > 0 {
		s.n.release(int64(removed))
	}
	return dst
}

// Len implements Store.
func (s *syncMap[T]) Len() int { return s.n.len() }

// Kind implements Store.
func (s *syncMap[T]) Kind() Kind { return SyncMap }

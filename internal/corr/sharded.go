package corr

import "sync"

// sharded is the reference store: a fixed set of mutex-guarded maps selected
// by a mix of the correlation id, so unrelated requests rarely contend. One
// shard (Config{Shards: 1}) is the plain locked map the bake-off starts from.
//
// Each shard also keeps the earliest deadline it holds, so a sweep that finds
// nothing due skips the shard without walking its map.
type sharded[T any] struct {
	shards []shard[T]
	mask   uint64
	n      counter
}

type shard[T any] struct {
	mu      sync.Mutex
	entries map[ID]entry[T]
	// earliest is the smallest deadline in entries, or maxDeadline when the
	// shard is empty. It is only ever lowered on Put, so it can be stale in
	// the safe direction: a sweep may walk a shard with nothing due, never
	// skip one with something due.
	earliest int64
	_        [cacheLine]byte
}

type entry[T any] struct {
	deadline int64
	value    T
}

// maxDeadline is the "no entry" marker for shard.earliest.
const maxDeadline = int64(^uint64(0) >> 1)

func newSharded[T any](cfg Config) *sharded[T] {
	s := &sharded[T]{
		shards: make([]shard[T], cfg.Shards),
		mask:   uint64(cfg.Shards - 1),
	}
	s.n.cap = int64(cfg.Capacity)
	per := cfg.Capacity/cfg.Shards + 1
	if per > 1024 {
		per = 1024
	}
	for i := range s.shards {
		s.shards[i].entries = make(map[ID]entry[T], per)
		s.shards[i].earliest = maxDeadline
	}
	return s
}

func (s *sharded[T]) shardFor(id ID) *shard[T] { return &s.shards[mix(id)&s.mask] }

// Put implements Store. The slot is reserved on the counter before the entry
// goes into its shard and released when it does not, so the capacity bound
// holds under concurrency and Len never counts an entry that is already gone.
func (s *sharded[T]) Put(id ID, deadline int64, v T) bool {
	if id == 0 || !s.n.reserve() {
		return false
	}
	sh := s.shardFor(id)
	sh.mu.Lock()
	if _, ok := sh.entries[id]; ok {
		sh.mu.Unlock()
		s.n.release(1)
		return false
	}
	sh.entries[id] = entry[T]{deadline: deadline, value: v}
	if deadline < sh.earliest {
		sh.earliest = deadline
	}
	sh.mu.Unlock()
	return true
}

// Take implements Store.
func (s *sharded[T]) Take(id ID) (T, bool) {
	sh := s.shardFor(id)
	sh.mu.Lock()
	e, ok := sh.entries[id]
	if ok {
		delete(sh.entries, id)
	}
	sh.mu.Unlock()
	if !ok {
		var zero T
		return zero, false
	}
	s.n.release(1)
	return e.value, true
}

// Expire implements Store.
func (s *sharded[T]) Expire(now int64, dst []Pending[T]) []Pending[T] {
	removed := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		if sh.earliest > now || len(sh.entries) == 0 {
			sh.mu.Unlock()
			continue
		}
		earliest := maxDeadline
		for id, e := range sh.entries {
			if e.deadline <= now {
				dst = append(dst, Pending[T]{ID: id, Deadline: e.deadline, Value: e.value})
				delete(sh.entries, id)
				removed++
				continue
			}
			if e.deadline < earliest {
				earliest = e.deadline
			}
		}
		sh.earliest = earliest
		sh.mu.Unlock()
	}
	if removed > 0 {
		s.n.release(int64(removed))
	}
	return dst
}

// Len implements Store.
func (s *sharded[T]) Len() int { return s.n.len() }

// Kind implements Store.
func (s *sharded[T]) Kind() Kind { return Sharded }

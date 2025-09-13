package timer

import "time"

// heap is a d-ary min-heap over entry pointers; Entry.idx tracks the
// position so Cancel is O(log n). Wheel kinds embed it for their far or
// overflow entries and use push/pop/top directly, which never touch Gen.
type heap[V any] struct {
	items []*Entry[V]
	arity int
}

func (h *heap[V]) Add(e *Entry[V], due int64) {
	e.checkFree()
	e.arm(due, whereHeap)
	h.push(e)
}

func (h *heap[V]) Cancel(e *Entry[V]) bool {
	if e.where != whereHeap {
		return false
	}
	h.removeAt(int(e.idx))
	e.disarm()
	return true
}

func (h *heap[V]) Next() (int64, bool) {
	if len(h.items) == 0 {
		return 0, false
	}
	return h.items[0].due, true
}

func (h *heap[V]) Fire(now int64, dst []*Entry[V]) []*Entry[V] {
	for len(h.items) > 0 && h.items[0].due <= now {
		e := h.pop()
		e.disarm()
		dst = append(dst, e)
	}
	return dst
}

func (h *heap[V]) Len() int { return len(h.items) }

func (h *heap[V]) Resolution() time.Duration { return 0 }

// push files e (whose due is set) and marks it as living in the heap.
func (h *heap[V]) push(e *Entry[V]) {
	e.where = whereHeap
	e.idx = int32(len(h.items))
	h.items = append(h.items, e)
	h.up(int(e.idx))
}

// top returns the earliest entry or nil.
func (h *heap[V]) top() *Entry[V] {
	if len(h.items) == 0 {
		return nil
	}
	return h.items[0]
}

// pop removes and returns the earliest entry; the caller re-files or
// disarms it.
func (h *heap[V]) pop() *Entry[V] {
	e := h.items[0]
	h.removeAt(0)
	return e
}

func (h *heap[V]) less(i, j int) bool { return h.items[i].due < h.items[j].due }

func (h *heap[V]) swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].idx = int32(i)
	h.items[j].idx = int32(j)
}

func (h *heap[V]) up(i int) {
	for i > 0 {
		p := (i - 1) / h.arity
		if !h.less(i, p) {
			return
		}
		h.swap(i, p)
		i = p
	}
}

func (h *heap[V]) down(i int) {
	n := len(h.items)
	for {
		first := i*h.arity + 1
		if first >= n {
			return
		}
		m := first
		last := min(first+h.arity, n)
		for c := first + 1; c < last; c++ {
			if h.less(c, m) {
				m = c
			}
		}
		if !h.less(m, i) {
			return
		}
		h.swap(i, m)
		i = m
	}
}

// removeAt deletes the entry at i, keeping the heap property.
func (h *heap[V]) removeAt(i int) {
	last := len(h.items) - 1
	if i != last {
		h.swap(i, last)
	}
	h.items[last] = nil
	h.items = h.items[:last]
	if i < last {
		h.down(i)
		h.up(i)
	}
}
